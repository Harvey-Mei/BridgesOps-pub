import { AssignmentDestinationPicker } from "@/components/gateway/assignment-destination-picker"
import { AssignmentQuickAction } from "@/components/gateway/assignment-quick-action"
import { AutomaticGroupAssignmentDialog } from "@/components/gateway/automatic-group-assignment-dialog"
import { useEffect, useMemo, useRef, useState } from "react"
import { Link } from "react-router-dom"
import { AlertTriangle, ArrowLeft, Ban, BellRing, CheckCircle2, ChevronLeft, ChevronRight, CircleHelp, Clock3, Copy, History, Inbox, RefreshCw, Search } from "lucide-react"
import { toast } from "sonner"
import type { GatewayCandidateGroup, GatewayUpstreamGroupChange, GatewayUpstreamGroupChangeHistoryCursorPage } from "@/lib/api-types"
import { apiFetch } from "@/lib/api"
import { dateTime, formatRatio } from "@/lib/format"
import {
  buildUpstreamGroupChangeFilterOptions,
  filterUpstreamGroupChanges,
  filterUpstreamGroupChangesByPendingFacet,
  groupUpstreamGroupChangesByChannelAndSource,
  reconcileUpstreamGroupChangeSelection,
  upstreamGroupChangeAssignmentStateLabel,
  upstreamGroupChangeChannelLabel,
  upstreamGroupChangeFilterAll,
  upstreamGroupChangeHistoryPath,
  upstreamGroupChangeIsActionable,
  upstreamGroupChangeManualReviewReasonLabel,
  upstreamGroupChangePendingFacetCounts,
  upstreamGroupChangeReasonLabel,
  upstreamGroupChangeRequiresManualRemovalReview,
  upstreamGroupChangeSelectionState,
  upstreamGroupChangeSourceLabel,
  type UpstreamGroupChangeFilterOptions,
  type UpstreamGroupChangeFilters,
  type UpstreamGroupChangePendingFacet,
  type UpstreamGroupChangePendingFacetCounts,
  type UpstreamGroupChangeView,
} from "@/lib/upstream-group-changes"
import {
  createUpstreamGroupChangeHistoryRequestGate,
  initialUpstreamGroupChangeHistoryCursorState,
  nextUpstreamGroupChangeHistoryCursorState,
  previousUpstreamGroupChangeHistoryCursorState,
  type UpstreamGroupChangeHistoryCursorState,
} from "@/lib/upstream-group-change-history-navigation"
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { PageHeader } from "@/components/ui/page-header"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"

type BatchResolveResult = {
  resolved: number
  failures: Record<string, string>
}

type PendingBatchAction = {
  ignore: boolean
  targets: GatewayUpstreamGroupChange[]
  candidateGroupIDs: number[]
}

type BatchFailureItem = {
  id: number
  label: string
  reason: string
}

const emptyFilters: UpstreamGroupChangeFilters = {
  search: "",
  channel: upstreamGroupChangeFilterAll,
  group: upstreamGroupChangeFilterAll,
  source: upstreamGroupChangeFilterAll,
}

const historyPageSize = 50

function initialHistoryCursorStateByView(): Record<"review" | "history", UpstreamGroupChangeHistoryCursorState> {
  return { review: initialUpstreamGroupChangeHistoryCursorState(), history: initialUpstreamGroupChangeHistoryCursorState() }
}

export function UpstreamGroupChangesWorkspace({ changes, candidateGroups, manualReviewCount, manualReviewError = null, syncingChannelID, onChanged, onRefresh, onSyncModels, onReconcileReserved, initialLoading = false, initialError = null }: {
  changes: GatewayUpstreamGroupChange[]
  candidateGroups: GatewayCandidateGroup[]
  manualReviewCount: number
  manualReviewError?: string | null
  syncingChannelID: number | null
  onChanged: () => void
  onRefresh: () => Promise<void>
  onSyncModels: (channelID: number) => Promise<void>
  onReconcileReserved: () => Promise<{ resolved: number; failures: number }>
  initialLoading?: boolean
  initialError?: string | null
}) {
  const [automationLocked, setAutomationLocked] = useState(false)
  const [view, setView] = useState<UpstreamGroupChangeView>("pending")
  const [pendingFacet, setPendingFacet] = useState<UpstreamGroupChangePendingFacet>("all")
  const [filters, setFilters] = useState<UpstreamGroupChangeFilters>(emptyFilters)
  const [selectedChangeIDs, setSelectedChangeIDs] = useState<number[]>([])
  const [batchSaving, setBatchSaving] = useState<"assign" | "ignore" | null>(null)
  const [refreshing, setRefreshing] = useState(false)
  const [batchFailures, setBatchFailures] = useState<BatchFailureItem[]>([])
  const [pendingBatchAction, setPendingBatchAction] = useState<PendingBatchAction | null>(null)
  const [historyCursorByView, setHistoryCursorByView] = useState<Record<"review" | "history", UpstreamGroupChangeHistoryCursorState>>(initialHistoryCursorStateByView)
  const [historyResult, setHistoryResult] = useState<{ key: string; response: GatewayUpstreamGroupChangeHistoryCursorPage } | null>(null)
  const [historyLoadingKey, setHistoryLoadingKey] = useState<string | null>(null)
  const [historyErrorKey, setHistoryErrorKey] = useState<string | null>(null)
  const [historyErrorReason, setHistoryErrorReason] = useState("")
  const [historyRefreshVersion, setHistoryRefreshVersion] = useState(0)
  const historyRequestGateRef = useRef(createUpstreamGroupChangeHistoryRequestGate())
  const [historyNavigationTargetKey, setHistoryNavigationTargetKey] = useState<string | null>(null)
  const batchInFlightRef = useRef(false)
  const refreshInFlightRef = useRef(false)
  const onRefreshRef = useRef(onRefresh)
  const [pendingRefreshError, setPendingRefreshError] = useState("")
  const [pendingManualReviewChange, setPendingManualReviewChange] = useState<GatewayUpstreamGroupChange | null>(null)
  const [manualReviewReason, setManualReviewReason] = useState("")
  const [manualReviewSavingID, setManualReviewSavingID] = useState<number | null>(null)
  const [manualReviewFailures, setManualReviewFailures] = useState<Record<number, string>>({})
  const [reconcilingReserved, setReconcilingReserved] = useState(false)

  const pendingAdded = useMemo(() => changes.filter((change) => change.kind === "added" && change.status === "pending"), [changes])
  const historyView = view === "pending" ? null : view
  const activeHistoryCursorState = historyView ? historyCursorByView[historyView] : initialUpstreamGroupChangeHistoryCursorState()
  const activeHistoryPage = activeHistoryCursorState.page
  const historyRequestPath = useMemo(() => historyView ? upstreamGroupChangeHistoryPath(historyView, activeHistoryCursorState.cursor, historyPageSize, filters) : "", [activeHistoryCursorState.cursor, filters, historyView])
  const historyRequestKey = historyRequestPath ? `${historyRequestPath}#${historyRefreshVersion}` : ""
  const activeHistoryResponse = historyResult?.key === historyRequestKey ? historyResult.response : null
  const historyItems = activeHistoryResponse?.items ?? []
  const historyLoaded = activeHistoryResponse !== null
  const historyNavigationLocked = historyNavigationTargetKey !== null
  const historyLoading = historyNavigationLocked || historyLoadingKey === historyRequestKey
  const historyFailed = historyErrorKey === historyRequestKey
  const historyHasPrevious = activeHistoryCursorState.previousCursors.length > 0
  const historyHasNext = Boolean(activeHistoryResponse?.next_cursor)
  const filteredPendingChanges = useMemo(() => filterUpstreamGroupChanges(pendingAdded, filters), [filters, pendingAdded])
  const pendingFacetCounts = useMemo(() => upstreamGroupChangePendingFacetCounts(filteredPendingChanges), [filteredPendingChanges])
  const visiblePendingChanges = useMemo(() => filterUpstreamGroupChangesByPendingFacet(filteredPendingChanges, pendingFacet), [filteredPendingChanges, pendingFacet])
  const displayedChanges = view === "pending" ? visiblePendingChanges : historyItems
  const pendingFilterOptions = useMemo(() => buildUpstreamGroupChangeFilterOptions(pendingAdded), [pendingAdded])
  const historyFilterOptions = activeHistoryResponse?.filter_options ?? buildUpstreamGroupChangeFilterOptions(historyItems)
  const filterOptions = view === "pending" ? pendingFilterOptions : historyFilterOptions
  const selectedIDSet = useMemo(() => new Set(selectedChangeIDs), [selectedChangeIDs])
  const selectedAdded = useMemo(() => pendingAdded.filter((change) => selectedIDSet.has(change.id) && upstreamGroupChangeIsActionable(change)), [pendingAdded, selectedIDSet])
  const candidateGroupByID = useMemo(() => new Map(candidateGroups.map((group) => [group.id, group])), [candidateGroups])
  const manualReviewReasonByteLength = useMemo(() => new TextEncoder().encode(manualReviewReason.trim()).length, [manualReviewReason])
  const interactionLocked = batchSaving !== null || pendingBatchAction !== null || automationLocked

  useEffect(() => {
    onRefreshRef.current = onRefresh
  }, [onRefresh])

  useEffect(() => {
    setSelectedChangeIDs((current) => {
      const next = reconcileUpstreamGroupChangeSelection(current, pendingAdded)
      return next.length === current.length ? current : next
    })
  }, [pendingAdded])

  useEffect(() => {
    if (!historyView || !historyRequestPath || !historyRequestKey) return
    const controller = new AbortController()
    const requestGate = historyRequestGateRef.current
    const request = requestGate.beginRequest(historyRequestKey)
    const requestIsCurrent = () => (
      !controller.signal.aborted
      && requestGate.matchesCurrentRequest(request, historyRequestKey)
    )
    setHistoryLoadingKey(historyRequestKey)
    setHistoryErrorKey((current) => current === historyRequestKey ? null : current)
    const delay = filters.search.trim() ? 200 : 0
    const timeout = window.setTimeout(() => {
      void apiFetch<GatewayUpstreamGroupChangeHistoryCursorPage>(historyRequestPath, { signal: controller.signal })
        .then((response) => {
          if (requestIsCurrent()) setHistoryResult({ key: historyRequestKey, response })
        })
        .catch((error: unknown) => {
          if (!requestIsCurrent()) return
          setHistoryErrorKey(historyRequestKey)
          setHistoryErrorReason(error instanceof Error ? error.message : "加载上游变更历史失败")
          toast.error("加载上游变更历史失败，请查看异常详情")
        })
        .finally(() => {
          if (!requestIsCurrent()) return
          setHistoryLoadingKey((current) => current === historyRequestKey ? null : current)
          if (requestGate.completeRequest(request, historyRequestKey)) {
            setHistoryNavigationTargetKey((current) => current === historyRequestKey ? null : current)
          }
        })
    }, delay)
    return () => {
      window.clearTimeout(timeout)
      controller.abort()
      requestGate.cancelRequest(request)
    }
  }, [filters.search, historyRequestKey, historyRequestPath, historyView])

  function invalidateHistoryNavigation() {
    historyRequestGateRef.current.invalidateNavigation()
    setHistoryNavigationTargetKey(null)
  }

  function beginHistoryNavigation(targetRequestKey: string) {
    if (!historyRequestGateRef.current.startNavigation(targetRequestKey)) return false
    setHistoryNavigationTargetKey(targetRequestKey)
    return true
  }

  function handleViewChange(value: string) {
    invalidateHistoryNavigation()
    const next = value as UpstreamGroupChangeView
    setView(next)
    setFilters(emptyFilters)
    setPendingFacet("all")
    setSelectedChangeIDs([])
    if (next !== "pending") {
      setHistoryCursorByView((current) => ({ ...current, [next]: initialUpstreamGroupChangeHistoryCursorState() }))
    }
  }

  function updateFilter(key: keyof UpstreamGroupChangeFilters, value: string) {
    invalidateHistoryNavigation()
    setFilters((current) => ({ ...current, [key]: value }))
    if (view !== "pending") setHistoryCursorByView((current) => ({ ...current, [view]: initialUpstreamGroupChangeHistoryCursorState() }))
  }

  function showPreviousHistoryPage() {
    if (!historyView) return
    const previousState = previousUpstreamGroupChangeHistoryCursorState(activeHistoryCursorState)
    if (previousState === activeHistoryCursorState) return
    const targetRequestKey = `${upstreamGroupChangeHistoryPath(historyView, previousState.cursor, historyPageSize, filters)}#${historyRefreshVersion}`
    if (!beginHistoryNavigation(targetRequestKey)) return
    setHistoryCursorByView((current) => ({ ...current, [historyView]: previousState }))
  }

  function showNextHistoryPage() {
    if (!historyView) return
    const nextCursor = String(activeHistoryResponse?.next_cursor ?? "").trim()
    if (!nextCursor) return
    const nextState = nextUpstreamGroupChangeHistoryCursorState(activeHistoryCursorState, nextCursor)
    if (nextState === activeHistoryCursorState) return
    const targetRequestKey = `${upstreamGroupChangeHistoryPath(historyView, nextState.cursor, historyPageSize, filters)}#${historyRefreshVersion}`
    if (!beginHistoryNavigation(targetRequestKey)) return
    setHistoryCursorByView((current) => ({ ...current, [historyView]: nextState }))
  }

  function retryHistory() {
    invalidateHistoryNavigation()
    setHistoryErrorKey(null)
    setHistoryErrorReason("")
    setHistoryRefreshVersion((current) => current + 1)
    void onRefreshRef.current().catch((error) => {
      toast.error(error instanceof Error ? error.message : "刷新上游变更摘要失败")
    })
  }

  function toggleChanges(targets: GatewayUpstreamGroupChange[], checked: boolean) {
    const targetIDs = new Set(targets.map((change) => change.id))
    const actionableTargetIDs = new Set(targets.filter(upstreamGroupChangeIsActionable).map((change) => change.id))
    setSelectedChangeIDs((current) => {
      const next = new Set(current)
      for (const id of (checked ? actionableTargetIDs : targetIDs)) {
        if (checked) next.add(id)
        else next.delete(id)
      }
      return Array.from(next)
    })
  }

  function requestIgnoreConfirmation() {
    if (selectedAdded.length === 0) return
    setPendingBatchAction({ ignore: true, targets: [...selectedAdded], candidateGroupIDs: [] })
  }

  async function resolveBatch(action: PendingBatchAction) {
    if (batchInFlightRef.current) return
    const currentByID = new Map(pendingAdded.map(change => [change.id, change]))
    const targets = action.targets.filter(change => {
      const current = currentByID.get(change.id)
      return current && upstreamGroupChangeIsActionable(current) && current.upstream_group_id === change.upstream_group_id && current.presence_generation === change.presence_generation
    })
    if (targets.length !== action.targets.length) {
      toast.error("选中变更已更新，请重新打开目标分组后操作")
      return
    }
    if (!action.ignore && (action.candidateGroupIDs.length === 0 || action.candidateGroupIDs.some(id => !candidateGroupByID.has(id)))) {
      toast.error("目标分组已更新，请重新选择")
      return
    }
    if (targets.length === 0) {
      toast.error("选中的上游变更当前均不可操作")
      return
    }
    batchInFlightRef.current = true
    setBatchSaving(action.ignore ? "ignore" : "assign")
    setBatchFailures([])
    try {
      const result = await apiFetch<BatchResolveResult>("/gateway/upstream-group-changes/resolve-batch", {
        method: "POST",
        body: JSON.stringify({ ids: targets.map((change) => change.id), candidate_group_ids: action.candidateGroupIDs, ignore: action.ignore }),
      })
      const failedIDs = new Set(Object.keys(result.failures ?? {}).map(Number))
      const targetIDs = new Set(targets.map((change) => change.id))
      const targetByID = new Map(targets.map((change) => [change.id, change]))
      setSelectedChangeIDs((current) => Array.from(new Set([...current.filter((id) => !targetIDs.has(id)), ...failedIDs])))
      setBatchFailures(Object.entries(result.failures ?? {}).map(([rawID, reason]) => {
        const id = Number(rawID)
        const change = targetByID.get(id)
        return { id, label: change ? `${upstreamGroupChangeChannelLabel(change)} / ${change.group_name}` : `变更 #${rawID}`, reason }
      }))
      if (result.resolved > 0) {
        onChanged()
        invalidateHistoryNavigation()
        setHistoryRefreshVersion((current) => current + 1)
        setHistoryCursorByView(initialHistoryCursorStateByView())
      }
      try {
        await onRefresh()
      } catch (error) {
        toast.error(error instanceof Error ? error.message : "刷新上游变更失败")
      }
      if (failedIDs.size > 0) {
        toast.warning(`已处理 ${result.resolved} 项，${failedIDs.size} 项失败，请查看上方失败详情`)
      } else {
        toast.success(action.ignore ? `已忽略 ${result.resolved} 项` : `已处理 ${result.resolved} 项，目标：${candidateGroupNames(action.candidateGroupIDs, candidateGroupByID)}`)
      }
    } catch (error) {
      const reason = error instanceof Error ? error.message : "批量处理上游变更失败"
      setSelectedChangeIDs(current => Array.from(new Set([...current, ...targets.map(change => change.id)])))
      setBatchFailures([{ id: 0, label: "批量请求", reason }])
      toast.error("处理结果未确认，请刷新后查看；本次不会自动重试")
      try { await onRefresh() } catch { /* Keep the original failure available for inspection. */ }
    } finally {
      batchInFlightRef.current = false
      setBatchSaving(null)
      setPendingBatchAction(null)
    }
  }

  function confirmBatchAction() {
    if (!pendingBatchAction) return
    void resolveBatch(pendingBatchAction)
  }

  function cancelBatchAction() {
    if (!pendingBatchAction || batchSaving !== null) return
    setPendingBatchAction(null)
  }

  function requestManualReview(change: GatewayUpstreamGroupChange) {
    setPendingManualReviewChange(change)
    setManualReviewReason("")
  }

  async function confirmManualReview() {
    const change = pendingManualReviewChange
    const reason = manualReviewReason.trim()
    if (!change || manualReviewSavingID !== null) return
    if (!reason) {
      toast.error("请填写人工复核说明")
      return
    }
    if (manualReviewReasonByteLength > 64) {
      toast.error("人工复核说明不能超过 64 字节")
      return
    }
    setManualReviewSavingID(change.id)
    try {
      await apiFetch(`/gateway/upstream-group-changes/${change.id}/manual-review/resume`, {
        method: "POST",
        body: JSON.stringify({ resolution: reason }),
      })
      setManualReviewFailures((current) => {
        const next = { ...current }
        delete next[change.id]
        return next
      })
      setPendingManualReviewChange(null)
      setManualReviewReason("")
      invalidateHistoryNavigation()
      setHistoryCursorByView(initialHistoryCursorStateByView())
      setHistoryRefreshVersion((current) => current + 1)
      toast.success("已重新核验并处理受管密钥删除")
      try {
        await onRefreshRef.current()
      } catch {
        toast.warning("删除已处理，待处理列表刷新失败，请稍后重试")
      }
    } catch (error) {
      const failure = error instanceof Error ? error.message : "人工复核处理失败"
      setManualReviewFailures((current) => ({ ...current, [change.id]: failure }))
      setPendingManualReviewChange(null)
      toast.error("人工复核未完成，请查看异常详情")
    } finally {
      setManualReviewSavingID(null)
    }
  }

  async function refreshPendingChanges(notify = true) {
    if (refreshInFlightRef.current) return
    refreshInFlightRef.current = true
    setRefreshing(true)
    try {
      await onRefreshRef.current()
      setPendingRefreshError("")
      if (notify) toast.success("待处理分组事实已刷新")
    } catch (error) {
      setPendingRefreshError(error instanceof Error ? error.message : "刷新待处理分组失败")
      if (notify) toast.error("刷新待处理分组失败，请查看异常详情")
    } finally {
      refreshInFlightRef.current = false
      setRefreshing(false)
    }
  }

  async function reconcileReservedReviews() {
    if (reconcilingReserved) return
    setReconcilingReserved(true)
    try {
      const result = await onReconcileReserved()
      toast.success(result.failures > 0 ? `已自动处理 ${result.resolved} 项，${result.failures} 项仍需核验` : `已自动处理 ${result.resolved} 项系统 Key`)
      await refreshPendingChanges(false)
      invalidateHistoryNavigation()
      setHistoryRefreshVersion((current) => current + 1)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "自动清理系统 Key 失败")
    } finally {
      setReconcilingReserved(false)
    }
  }

  const workspace = (
    <Tabs value={view} onValueChange={handleViewChange} className="flex min-h-[calc(100vh-13rem)] flex-1 gap-0 overflow-hidden rounded-xl border border-border bg-card">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border bg-muted/15 px-4 py-2 sm:px-6">
        <TabsList className="bg-muted/70">
          <TabsTrigger disabled={interactionLocked} value="pending"><Inbox className="size-3.5" aria-hidden="true" />{`待处理 ${pendingAdded.length}`}</TabsTrigger>
          <TabsTrigger disabled={interactionLocked} value="review"><AlertTriangle className="size-3.5" aria-hidden="true" />{`托管 Key 复核 ${manualReviewCount}`}</TabsTrigger>
          <TabsTrigger disabled={interactionLocked} value="history"><History className="size-3.5" aria-hidden="true" />{"处理记录"}</TabsTrigger>
        </TabsList>
        {manualReviewError ? <FailureDetailButton label="托管 Key 复核数量刷新失败" reason={manualReviewError} /> : null}
        {view === "review" ? <Button type="button" size="sm" variant="outline" className="h-8 gap-1.5 text-xs" disabled={reconcilingReserved} onClick={() => { void reconcileReservedReviews() }}><RefreshCw className={reconcilingReserved ? "size-3.5 animate-spin" : "size-3.5"} />{"自动清理系统 Key"}</Button> : null}
        {view === "pending" ? (
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <span>{`${pendingFacetCounts.actionable} 项可直接处理`}</span>
            {pendingFacetCounts.attention > 0 ? <Badge variant="outline" className="border-warning/30 text-warning">{`${pendingFacetCounts.attention} 项需关注`}</Badge> : null}
          </div>
        ) : null}
      </div>

      <TabsContent value="pending" className="min-h-0 overflow-hidden">
        {initialLoading ? <EmptyState icon={RefreshCw} label="正在加载上游变更与候选分组" spinning /> : null}
        {!initialLoading && initialError ? <LoadFailureState label="上游变更工作台加载失败" reason={initialError} retrying={refreshing} onRetry={() => { void refreshPendingChanges() }} /> : null}
        {!initialLoading && !initialError ? (
          <div className="flex h-full min-h-0 flex-col">
            <BatchFailurePanel failures={batchFailures} />
            <div className="flex min-h-0 flex-1 flex-col">
              <ChangeQueue
                changes={displayedChanges}
                allChanges={pendingAdded}
                facet={pendingFacet}
                facetCounts={pendingFacetCounts}
                filters={filters}
                filterOptions={filterOptions}
                selectedIDs={selectedIDSet}
                candidateGroupByID={candidateGroupByID}
                refreshing={refreshing}
                refreshFailure={pendingRefreshError}
                syncingChannelID={syncingChannelID}
                disabled={interactionLocked}
                onFilterChange={updateFilter}
                onFacetChange={setPendingFacet}
                onToggle={toggleChanges}
                onRefresh={() => { void refreshPendingChanges() }}
                onSyncModels={(channelID) => { void onSyncModels(channelID) }}
                onAssign={(targets, groupIDs) => resolveBatch({ ignore: false, targets, candidateGroupIDs: groupIDs })}
                emptyLabel={hasActiveFilters(filters) || pendingFacet !== "all" ? "没有匹配当前条件的待处理分组" : "暂无待处理分组"}
              />
            </div>
            {selectedAdded.length > 0 ? (
              <div className="sticky bottom-0 z-20 flex items-center justify-between gap-3 border-t border-border bg-card px-4 py-3 shadow-sm" role="toolbar" aria-label="已选待处理变更操作">
                <div className="flex items-center gap-3 text-sm"><span className="font-medium">已选 {selectedAdded.length} 项</span><Button size="sm" variant="ghost" disabled={interactionLocked} onClick={() => setSelectedChangeIDs([])}>清除选择</Button></div>
                <div className="flex items-center gap-2">
                  <Button size="sm" variant="outline" disabled={interactionLocked} onClick={requestIgnoreConfirmation}>忽略</Button>
                  <AssignmentDestinationPicker changes={selectedAdded} groups={candidateGroups} disabled={interactionLocked} visibleSelectedCount={displayedChanges.filter(change => selectedIDSet.has(change.id)).length} onAssign={(targets, groupIDs) => resolveBatch({ ignore: false, targets, candidateGroupIDs: groupIDs })} />
                </div>
              </div>
            ) : null}
          </div>
        ) : null}
      </TabsContent>

      <TabsContent value="review" className="min-h-0 overflow-hidden">
        <ChangeHistory
          changes={displayedChanges}
          filters={filters}
          filterOptions={filterOptions}
          filterOptionsTruncated={Boolean(activeHistoryResponse?.filter_options?.truncated)}
          loading={historyLoading}
          loaded={historyLoaded}
          failed={historyFailed}
          failureReason={historyErrorReason}
          view="review"
          page={activeHistoryPage}
          hasPrevious={historyHasPrevious}
          hasNext={historyHasNext}
          onFilterChange={updateFilter}
          onPrevious={showPreviousHistoryPage}
          onNext={showNextHistoryPage}
          onRetry={retryHistory}
          manualReviewSavingID={manualReviewSavingID}
          manualReviewFailures={manualReviewFailures}
          onRequestManualReview={requestManualReview}
          candidateGroupByID={candidateGroupByID}
        />
      </TabsContent>

      <TabsContent value="history" className="min-h-0 overflow-hidden">
        <ChangeHistory
          changes={displayedChanges}
          filters={filters}
          filterOptions={filterOptions}
          filterOptionsTruncated={Boolean(activeHistoryResponse?.filter_options?.truncated)}
          loading={historyLoading}
          loaded={historyLoaded}
          failed={historyFailed}
          failureReason={historyErrorReason}
          view="history"
          page={activeHistoryPage}
          hasPrevious={historyHasPrevious}
          hasNext={historyHasNext}
          onFilterChange={updateFilter}
          onPrevious={showPreviousHistoryPage}
          onNext={showNextHistoryPage}
          onRetry={retryHistory}
          manualReviewSavingID={manualReviewSavingID}
          manualReviewFailures={manualReviewFailures}
          onRequestManualReview={requestManualReview}
          candidateGroupByID={candidateGroupByID}
        />
      </TabsContent>
    </Tabs>
  )

  return (
    <>
      <section className="flex min-h-[calc(100vh-10rem)] flex-col gap-4">
        <PageHeader
          icon={BellRing}
          title="上游分组变更"
          description="处理新增分组、异常移除与候选分组归属。"
          actions={(
            <>
              {view === "pending" ? <>
                <AssignmentQuickAction
                  changes={selectedChangeIDs.length > 0 ? pendingAdded.filter(change => selectedIDSet.has(change.id)) : visiblePendingChanges}
                  groups={candidateGroups}
                  scopeLabel={selectedChangeIDs.length > 0 ? `已选 ${selectedChangeIDs.length} 项（包含筛选外的已选项）` : "当前筛选结果"}
                  disabled={interactionLocked || initialLoading || Boolean(initialError)}
                  onLockChange={setAutomationLocked}
                  onFinished={async applied => {
                    const appliedIDs = new Set(applied)
                    setSelectedChangeIDs(current => current.filter(id => !appliedIDs.has(id)))
                    invalidateHistoryNavigation()
                    setHistoryRefreshVersion(current => current + 1)
                    await onRefresh()
                  }}
                />
                <AutomaticGroupAssignmentDialog groups={candidateGroups} candidates={[]} disabled={interactionLocked} onChanged={() => { void refreshPendingChanges(false) }} />
              </> : null}
              <Button type="button" variant="outline" size="sm" asChild><Link to="/gateway/keys"><ArrowLeft className="size-3.5" />{"密钥管理"}</Link></Button>
              <Button type="button" variant="outline" size="sm" onClick={() => { if (view === "pending") void refreshPendingChanges(); else retryHistory() }} disabled={view === "pending" ? refreshing : historyLoading} aria-busy={view === "pending" ? refreshing : historyLoading}>
                <RefreshCw className={(view === "pending" ? refreshing : historyLoading) ? "size-3.5 animate-spin" : "size-3.5"} />{"刷新"}
              </Button>
            </>
          )}
        />
        {workspace}
      </section>

      <AlertDialog open={pendingBatchAction !== null} onOpenChange={(next) => { if (!next) cancelBatchAction() }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{pendingBatchAction?.ignore ? "确认忽略选中变更" : "确认加入候选分组"}</AlertDialogTitle>
            <AlertDialogDescription>
              {pendingBatchAction?.ignore
                ? `将忽略 ${pendingBatchAction.targets.length} 项待处理变更；这些项目会进入“已忽略”。`
                : `${pendingBatchAction?.targets.length ?? 0} 个上游分组 × ${pendingBatchAction?.candidateGroupIDs.length ?? 0} 个目标，将新增 ${(pendingBatchAction?.targets.length ?? 0) * (pendingBatchAction?.candidateGroupIDs.length ?? 0)} 条候选归属。`}
            </AlertDialogDescription>
          </AlertDialogHeader>
          {pendingBatchAction ? (
            <div className="space-y-2 rounded-md border border-border bg-muted/20 p-3 text-sm">
              <div className="flex items-center justify-between gap-3"><span className="text-muted-foreground">{"待处理项"}</span><span className="font-semibold tabular-nums">{pendingBatchAction.targets.length}</span></div>
              <div className="flex items-start justify-between gap-3"><span className="text-muted-foreground">{"目标"}</span><span className="max-w-[70%] text-right font-medium">{pendingBatchAction.ignore ? "已忽略" : candidateGroupNames(pendingBatchAction.candidateGroupIDs, candidateGroupByID)}</span></div>
              {!pendingBatchAction.ignore ? <div className="flex items-center justify-between gap-3"><span className="text-muted-foreground">{"归属变更"}</span><span className="font-semibold tabular-nums">{`新增 ${pendingBatchAction.targets.length * pendingBatchAction.candidateGroupIDs.length} 条，不移除现有归属`}</span></div> : null}
            </div>
          ) : null}
          <AlertDialogFooter>
            <AlertDialogCancel disabled={batchSaving !== null}>{"取消"}</AlertDialogCancel>
            <Button type="button" className={pendingBatchAction?.ignore ? "bg-destructive text-white hover:bg-destructive/90" : undefined} disabled={batchSaving !== null} aria-busy={batchSaving !== null} onClick={confirmBatchAction}>
              {batchSaving !== null ? <RefreshCw className="size-3.5 animate-spin" aria-hidden="true" /> : null}
              {batchSaving !== null ? "正在处理" : pendingBatchAction?.ignore ? "确认忽略" : "确认加入"}
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={pendingManualReviewChange !== null} onOpenChange={(next) => { if (!next && manualReviewSavingID === null) setPendingManualReviewChange(null) }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{"人工复核受管密钥删除"}</AlertDialogTitle>
            <AlertDialogDescription>
              {pendingManualReviewChange
                ? `将重新核验 ${upstreamGroupChangeChannelLabel(pendingManualReviewChange)} / ${pendingManualReviewChange.group_name} 的远端密钥身份；核验通过后会删除仍存在的精确受管密钥，或确认它已经不存在。`
                : "将重新核验受管密钥删除状态。"}
            </AlertDialogDescription>
          </AlertDialogHeader>
          {pendingManualReviewChange ? (
            <div className="space-y-3">
              <div className="rounded-md border border-warning/25 bg-warning/5 p-3 text-sm">
                <div className="font-medium">{"待复核原因"}</div>
                <p className="mt-1 whitespace-pre-wrap break-words text-xs text-muted-foreground">{upstreamGroupChangeManualReviewReasonLabel(pendingManualReviewChange)}</p>
              </div>
              <label className="block space-y-1.5 text-sm font-medium">
                <span>{"复核说明"}</span>
                <Input value={manualReviewReason} onChange={(event) => setManualReviewReason(event.target.value)} maxLength={64} placeholder="填写核对依据或工单编号" disabled={manualReviewSavingID !== null} aria-label="人工复核说明" />
                <span className={`block text-right text-[10px] font-normal tabular-nums ${manualReviewReasonByteLength > 64 ? "text-danger" : "text-muted-foreground"}`}>{`${manualReviewReasonByteLength} / 64 字节`}</span>
              </label>
            </div>
          ) : null}
          <AlertDialogFooter>
            <AlertDialogCancel disabled={manualReviewSavingID !== null}>{"取消"}</AlertDialogCancel>
            <Button type="button" disabled={manualReviewSavingID !== null || !manualReviewReason.trim() || manualReviewReasonByteLength > 64} aria-busy={manualReviewSavingID !== null} onClick={() => { void confirmManualReview() }}>
              {manualReviewSavingID !== null ? <RefreshCw className="size-3.5 animate-spin" aria-hidden="true" /> : null}
              {"重新核验并处理"}
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}

function BatchFailurePanel({ failures }: { failures: BatchFailureItem[] }) {
  if (failures.length === 0) return null
  return (
    <section className="border-b border-danger/20 bg-danger/5 px-4 py-2.5 sm:px-6" aria-label="批量处理失败原因" aria-live="polite">
      <div className="flex flex-wrap items-center gap-2">
        <AlertTriangle className="size-4 shrink-0 text-danger" aria-hidden="true" />
        <h4 className="text-xs font-semibold text-danger">{"部分项目处理失败"}</h4>
        <Badge variant="outline" className="border-danger/25 text-danger">{`${failures.length} 项`}</Badge>
        {failures.map((failure) => (
          <FailureDetailButton key={`${failure.id}-${failure.label}`} label={failure.label} reason={failure.reason} />
        ))}
      </div>
    </section>
  )
}

function ChangeQueue({ changes, allChanges, facet, facetCounts, filters, filterOptions, selectedIDs, candidateGroupByID, refreshing, refreshFailure, syncingChannelID, disabled, onFilterChange, onFacetChange, onToggle, onRefresh, onSyncModels, onAssign, emptyLabel }: {
  changes: GatewayUpstreamGroupChange[]
  allChanges: GatewayUpstreamGroupChange[]
  facet: UpstreamGroupChangePendingFacet
  facetCounts: UpstreamGroupChangePendingFacetCounts
  filters: UpstreamGroupChangeFilters
  filterOptions: UpstreamGroupChangeFilterOptions
  selectedIDs: Set<number>
  candidateGroupByID: Map<number, GatewayCandidateGroup>
  refreshing: boolean
  refreshFailure: string
  syncingChannelID: number | null
  disabled: boolean
  onFilterChange: (key: keyof UpstreamGroupChangeFilters, value: string) => void
  onFacetChange: (facet: UpstreamGroupChangePendingFacet) => void
  onToggle: (changes: GatewayUpstreamGroupChange[], checked: boolean) => void
  onRefresh: () => void
  onSyncModels: (channelID: number) => void
  onAssign: (targets: GatewayUpstreamGroupChange[], groupIDs: number[]) => Promise<void>
  emptyLabel: string
}) {
  const groups = groupUpstreamGroupChangesByChannelAndSource(changes)
  const selectedCount = allChanges.filter((change) => upstreamGroupChangeIsActionable(change) && selectedIDs.has(change.id)).length
  const visibleActionableChanges = changes.filter(upstreamGroupChangeIsActionable)

  return (
    <section className="flex min-h-0 min-w-0 flex-1 flex-col bg-background">
      <ChangeFilterToolbar filters={filters} filterOptions={filterOptions} onFilterChange={onFilterChange} refreshing={refreshing} refreshFailure={refreshFailure} onRefresh={onRefresh} disabled={disabled} />
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border bg-muted/10 px-4 py-2 sm:px-6">
        <PendingFacetTabs value={facet} counts={facetCounts} onChange={onFacetChange} disabled={disabled} />
        <div className="flex items-center gap-2 text-xs">
          {selectedCount > 0 ? <span className="font-medium text-brand tabular-nums">{`已选 ${selectedCount}`}</span> : null}
          <label className="flex h-8 cursor-pointer items-center gap-2 px-2 font-medium hover:bg-muted/40">
            <Checkbox checked={upstreamGroupChangeSelectionState(changes, selectedIDs)} onCheckedChange={(checked) => onToggle(changes, checked === true)} disabled={disabled || visibleActionableChanges.length === 0} aria-label="选择当前筛选结果" />
            <span>{`选择当前结果 ${visibleActionableChanges.length}`}</span>
          </label>
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-auto" role="table" aria-label="待处理上游分组变更">
        {groups.length === 0 ? <EmptyState icon={Inbox} label={emptyLabel} /> : (
          <div className="min-w-[68rem]">
            <div className="sticky top-0 z-[1] grid h-9 grid-cols-[2rem_minmax(10rem,1fr)_minmax(11rem,1.15fr)_7rem_minmax(12rem,1.35fr)_minmax(9rem,1fr)_8.5rem_9rem] items-center gap-3 border-b border-border bg-muted/90 px-4 text-[11px] font-medium text-muted-foreground backdrop-blur sm:px-6" role="row">
              <span aria-hidden="true" />
              <span role="columnheader">{"渠道 / 来源"}</span>
              <span role="columnheader">{"上游分组"}</span>
              <span role="columnheader">{"倍率"}</span>
              <span role="columnheader">{"判定"}</span>
              <span role="columnheader">{"建议目标"}</span>
              <span role="columnheader">{"发现时间"}</span>
              <span role="columnheader" className="text-right">{"操作"}</span>
            </div>
            {groups.flatMap((channel) => channel.sources.flatMap((source) => source.changes.map((change) => (
              <PendingChangeRow
                key={change.id}
                change={change}
                channelName={channel.channelName}
                sourceName={source.sourceName}
                selected={selectedIDs.has(change.id)}
                candidateGroupByID={candidateGroupByID}
                syncingChannelID={syncingChannelID}
                showSyncAction={channel.sources[0]?.changes[0]?.id === change.id}
                onSyncModels={onSyncModels}
                onAssign={onAssign}
                selectionDisabled={disabled}
                onToggle={onToggle}
              />
            ))))}
          </div>
        )}
      </div>
    </section>
  )
}

function PendingChangeRow({ change, channelName, sourceName, selected, candidateGroupByID, syncingChannelID, showSyncAction, onSyncModels, onAssign, selectionDisabled, onToggle }: {
  change: GatewayUpstreamGroupChange
  channelName: string
  sourceName: string
  selected: boolean
  candidateGroupByID: Map<number, GatewayCandidateGroup>
  syncingChannelID: number | null
  showSyncAction: boolean
  onSyncModels: (channelID: number) => void
  onAssign: (targets: GatewayUpstreamGroupChange[], groupIDs: number[]) => Promise<void>
  selectionDisabled: boolean
  onToggle: (changes: GatewayUpstreamGroupChange[], checked: boolean) => void
}) {
  const actionable = upstreamGroupChangeIsActionable(change)
  const toggle = () => {
    if (actionable && !selectionDisabled) onToggle([change], !selected)
  }
  return (
    <div className={`grid min-h-14 grid-cols-[2rem_minmax(10rem,1fr)_minmax(11rem,1.15fr)_7rem_minmax(12rem,1.35fr)_minmax(9rem,1fr)_8.5rem_9rem] items-center gap-3 border-b border-border/60 px-4 py-2 transition-colors sm:px-6 ${selected ? "bg-brand/5" : actionable ? "cursor-pointer hover:bg-muted/25" : "bg-warning/5"}`} onClick={toggle} role="row">
      <Checkbox checked={selected} disabled={selectionDisabled || !actionable} onClick={(event) => event.stopPropagation()} onCheckedChange={(checked) => onToggle([change], checked === true)} aria-label={`选择 ${channelName} / ${change.group_name}`} />
      <span className="min-w-0" role="cell">
        <span className="block truncate text-sm font-semibold" title={channelName}>{channelName}</span>
        <span className="block truncate text-[11px] text-muted-foreground" title={sourceName}>{sourceName}</span>
      </span>
      <span className="min-w-0" role="cell">
        <span className="block truncate text-sm font-medium" title={change.group_name}>{change.group_name}</span>
        <span className="block truncate text-[11px] text-muted-foreground">{"新增分组"}</span>
      </span>
      <span className="min-w-0 tabular-nums" role="cell" title={upstreamGroupChangeRatioTitle(change)}>
        <span className="block text-xs font-semibold">{`入 ${formatRatioWithUnit(change.ratio)}`}</span>
        <span className="block text-[11px] text-muted-foreground">{`出 ${formatRatioWithUnit(change.completion_ratio)}`}</span>
      </span>
      <span className="min-w-0" role="cell">
        <Badge variant="outline" className={`mb-0.5 h-5 max-w-full justify-center truncate px-1.5 text-[10px] ${!actionable || change.assignment_state === "conflict" || change.assignment_state === "unmatched" ? "border-warning/35 text-warning" : ""}`}>
          {!actionable ? "不可处理" : upstreamGroupChangeAssignmentStateLabel(change.assignment_state)}
        </Badge>
        <span className="block truncate text-[11px] text-muted-foreground" title={upstreamGroupChangeReasonLabel(change)}>{upstreamGroupChangeReasonLabel(change)}</span>
      </span>
      <span className="truncate text-xs text-muted-foreground" role="cell" title={suggestedTargetsLabel(change, candidateGroupByID)}>{suggestedTargetsLabel(change, candidateGroupByID)}</span>
      <span className="truncate text-xs text-muted-foreground" role="cell" title={dateTime(change.detected_at)}>{dateTime(change.detected_at)}</span>
      <span className="flex items-center justify-end gap-0.5" role="cell">
        <AssignmentDestinationPicker compact changes={[change]} groups={Array.from(candidateGroupByID.values())} disabled={selectionDisabled || !actionable} onAssign={onAssign} />
        {showSyncAction ? (
          <Tooltip>
            <TooltipTrigger asChild>
              <Button type="button" size="icon-sm" variant="ghost" className="size-7" disabled={selectionDisabled || syncingChannelID !== null} aria-busy={syncingChannelID === change.channel_id} aria-label={`同步 ${channelName} 模型事实`} onClick={(event) => { event.stopPropagation(); onSyncModels(change.channel_id) }}>
                <RefreshCw className={syncingChannelID === change.channel_id ? "size-3.5 animate-spin" : "size-3.5"} aria-hidden="true" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>{"同步该渠道模型事实"}</TooltipContent>
          </Tooltip>
        ) : null}
        <Popover>
          <PopoverTrigger asChild>
            <Button type="button" size="icon-sm" variant="ghost" className="size-7" title="查看变更详情" aria-label={`查看 ${channelName} / ${change.group_name} 变更详情`} onClick={(event) => event.stopPropagation()}>
              <CircleHelp className="size-3.5" aria-hidden="true" />
            </Button>
          </PopoverTrigger>
          <PopoverContent align="end" className="max-h-[min(28rem,calc(100vh-2rem))] w-[min(24rem,calc(100vw-2rem))] overflow-y-auto p-0" onClick={(event) => event.stopPropagation()}>
          <div className="border-b border-border px-3 py-2.5">
            <div className="break-all text-sm font-semibold">{`${channelName} / ${change.group_name}`}</div>
          </div>
          <dl className="grid grid-cols-[5rem_minmax(0,1fr)] gap-x-3 gap-y-2 px-3 py-3 text-xs">
            <dt className="text-muted-foreground">{"渠道"}</dt><dd className="break-all">{channelName}</dd>
            <dt className="text-muted-foreground">{"上游分组"}</dt><dd className="break-all">{change.group_name}</dd>
            <dt className="text-muted-foreground">{"当前状态"}</dt><dd className="font-medium">{!actionable ? "不可处理" : upstreamGroupChangeAssignmentStateLabel(change.assignment_state)}</dd>
            <dt className="text-muted-foreground">{"判定说明"}</dt><dd className="whitespace-pre-wrap break-words">{upstreamGroupChangeReasonLabel(change)}</dd>
            <dt className="text-muted-foreground">{"建议目标"}</dt><dd className="break-words">{suggestedTargetsLabel(change, candidateGroupByID)}</dd>
            <dt className="text-muted-foreground">{"来源"}</dt><dd className="break-all">{sourceName}</dd>
            <dt className="text-muted-foreground">{"输入倍率"}</dt><dd className="tabular-nums">{formatRatioWithUnit(change.ratio)}</dd>
            <dt className="text-muted-foreground">{"输出倍率"}</dt><dd className="tabular-nums">{formatRatioWithUnit(change.completion_ratio)}</dd>
            <dt className="text-muted-foreground">{"发现时间"}</dt><dd>{dateTime(change.detected_at)}</dd>
          </dl>
          </PopoverContent>
        </Popover>
      </span>
    </div>
  )
}

function PendingFacetTabs({ value, counts, disabled = false, onChange }: { value: UpstreamGroupChangePendingFacet; counts: UpstreamGroupChangePendingFacetCounts; disabled?: boolean; onChange: (value: UpstreamGroupChangePendingFacet) => void }) {
  const facets: { value: UpstreamGroupChangePendingFacet; label: string }[] = [
    { value: "all", label: "全部" },
    { value: "actionable", label: "可处理" },
    { value: "awaiting", label: "等待事实" },
    { value: "attention", label: "冲突/不可用" },
  ]
  return (
    <div className="inline-flex h-8 max-w-full items-center overflow-x-auto rounded-md bg-muted p-[3px] text-muted-foreground" role="group" aria-label="筛选待处理状态">
      {facets.map((facet) => (
          <button key={facet.value} type="button" disabled={disabled} className={`h-6 min-w-max rounded px-2 text-xs font-medium outline-none transition-[color,box-shadow] focus-visible:ring-2 focus-visible:ring-ring ${value === facet.value ? "bg-background text-foreground shadow-sm" : "hover:text-foreground"} disabled:cursor-not-allowed disabled:opacity-50`} aria-pressed={value === facet.value} onClick={() => onChange(facet.value)}>
          {`${facet.label} ${counts[facet.value]}`}
        </button>
      ))}
    </div>
  )
}

function ChangeHistory({ changes, filters, filterOptions, filterOptionsTruncated, loading, loaded, failed, failureReason, view, page, hasPrevious, hasNext, manualReviewSavingID, manualReviewFailures, candidateGroupByID, onFilterChange, onPrevious, onNext, onRetry, onRequestManualReview }: {
  changes: GatewayUpstreamGroupChange[]
  filters: UpstreamGroupChangeFilters
  filterOptions: UpstreamGroupChangeFilterOptions
  filterOptionsTruncated: boolean
  loading: boolean
  loaded: boolean
  failed: boolean
  failureReason: string
  view: "review" | "history"
  page: number
  hasPrevious: boolean
  hasNext: boolean
  manualReviewSavingID: number | null
  manualReviewFailures: Record<number, string>
  candidateGroupByID: Map<number, GatewayCandidateGroup>
  onFilterChange: (key: keyof UpstreamGroupChangeFilters, value: string) => void
  onPrevious: () => void
  onNext: () => void
  onRetry: () => void
  onRequestManualReview: (change: GatewayUpstreamGroupChange) => void
}) {
  return (
    <div className="flex h-full min-h-0 flex-col">
      <ChangeFilterToolbar filters={filters} filterOptions={filterOptions} onFilterChange={onFilterChange} refreshing={loading} refreshLabel={view === "review" ? "待人工复核" : "处理记录"} onRefresh={onRetry} />
      {filterOptionsTruncated ? <div className="border-b border-warning/20 bg-warning/5 px-4 py-2 text-[11px] text-muted-foreground sm:px-6">{"筛选项超过 500 个，仅展示前 500 个；上方搜索仍会查询全部历史。"}</div> : null}
      <div className="min-h-0 flex-1 overflow-auto">
        {loading && !loaded ? <EmptyState icon={RefreshCw} label="正在加载处理记录" spinning /> : null}
        {!loading && failed ? <LoadFailureState label="处理记录加载失败" reason={failureReason} retrying={loading} onRetry={onRetry} /> : null}
        {!loading && loaded && changes.length === 0 ? <EmptyState icon={view === "review" ? AlertTriangle : History} label={hasActiveFilters(filters) ? `没有匹配的${view === "review" ? "待复核事项" : "处理记录"}` : `暂无${view === "review" ? "待复核事项" : "处理记录"}`} /> : null}
        {loaded && changes.length > 0 ? <div className="min-w-[58rem]">
          <div className="sticky top-0 z-[1] grid h-9 grid-cols-[2rem_minmax(11rem,1.2fr)_minmax(10rem,1fr)_minmax(11rem,1.2fr)_minmax(9rem,1fr)_9rem_5rem] items-center gap-3 border-b border-border bg-muted/90 px-4 text-[11px] font-medium text-muted-foreground backdrop-blur sm:px-6" role="row">
            <span aria-hidden="true" /><span>{"渠道 / 来源"}</span><span>{"上游分组"}</span><span>{"状态 / 原因"}</span><span>{"处理信息"}</span><span>{"时间"}</span><span className="text-right">{"操作"}</span>
          </div>
          {changes.map((change) => {
            const requiresManualReview = upstreamGroupChangeRequiresManualRemovalReview(change)
            const manualReviewFailure = manualReviewFailures[change.id]
            const manualReviewSaving = manualReviewSavingID === change.id
            return (
              <div key={change.id} className="grid min-h-14 grid-cols-[2rem_minmax(11rem,1.2fr)_minmax(10rem,1fr)_minmax(11rem,1.2fr)_minmax(9rem,1fr)_9rem_5rem] items-center gap-3 border-b border-border/60 px-4 py-2 hover:bg-muted/20 sm:px-6" role="row">
                <ChangeStatusIndicator change={change} />
                <div className="min-w-0">
                  <div className="truncate text-sm font-semibold" title={upstreamGroupChangeChannelLabel(change)}>{upstreamGroupChangeChannelLabel(change)}</div>
                  <div className="truncate text-[11px] text-muted-foreground" title={upstreamGroupChangeSourceLabel(change)}>{upstreamGroupChangeSourceLabel(change)}</div>
                </div>
                <div className="min-w-0"><div className="truncate text-sm font-medium" title={change.group_name}>{change.group_name}</div><div className="text-[11px] text-muted-foreground">{change.kind === "added" ? "新增分组" : "移除分组"}</div></div>
                <div className="min-w-0">
                  <Badge variant="outline" className={change.status === "ignored" ? "border-warning/30 text-warning" : "border-success/30 text-success"}>{requiresManualReview ? "需人工复核" : historyStatusLabel(change)}</Badge>
                  <div className="mt-0.5 truncate text-[11px] text-muted-foreground" title={requiresManualReview ? upstreamGroupChangeManualReviewReasonLabel(change) : upstreamGroupChangeReasonLabel(change)}>{requiresManualReview ? upstreamGroupChangeManualReviewReasonLabel(change) : upstreamGroupChangeReasonLabel(change)}</div>
                </div>
                <div className="min-w-0 text-xs">
                  <div className="truncate font-medium" title={historyActorLabel(change)}>{historyActorLabel(change)}</div>
                  <div className="truncate text-[11px] text-muted-foreground" title={historyTargetsLabel(change, candidateGroupByID)}>{historyTargetsLabel(change, candidateGroupByID)}</div>
                </div>
                <div className="text-xs text-muted-foreground"><div>{dateTime(change.resolved_at ?? change.detected_at)}</div><div className="text-[11px]">{change.resolved_at ? "处理时间" : "发现时间"}</div></div>
                <div className="flex items-center justify-end gap-1.5">
                  <HistoryDetailButton change={change} candidateGroupByID={candidateGroupByID} />
                  {manualReviewFailure ? (
                    <FailureDetailButton label={`${upstreamGroupChangeChannelLabel(change)} / ${change.group_name}`} reason={manualReviewFailure} />
                  ) : null}
                  {requiresManualReview ? (
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <Button type="button" size="icon-sm" variant="outline" disabled={manualReviewSavingID !== null} aria-busy={manualReviewSaving} aria-label={`人工复核 ${upstreamGroupChangeChannelLabel(change)} / ${change.group_name}`} onClick={() => onRequestManualReview(change)}>
                          <RefreshCw className={manualReviewSaving ? "size-3.5 animate-spin" : "size-3.5"} aria-hidden="true" />
                        </Button>
                      </TooltipTrigger>
                      <TooltipContent>{"重新核验并处理受管密钥删除"}</TooltipContent>
                    </Tooltip>
                  ) : null}
                </div>
              </div>
            )
          })}
        </div> : null}
      </div>
      {loaded && (hasPrevious || hasNext) ? (
        <div className="flex items-center justify-end gap-2 border-t border-border px-4 py-3 sm:px-6">
          <span className="mr-1 text-xs text-muted-foreground">{`第 ${page} 页`}</span>
          <Tooltip>
            <TooltipTrigger asChild><span><Button type="button" size="icon-sm" variant="outline" disabled={loading || !hasPrevious} onClick={onPrevious} aria-label="上一页历史"><ChevronLeft className="size-3.5" aria-hidden="true" /></Button></span></TooltipTrigger>
            <TooltipContent>{"上一页历史"}</TooltipContent>
          </Tooltip>
          <Tooltip>
            <TooltipTrigger asChild><span><Button type="button" size="icon-sm" variant="outline" disabled={loading || !hasNext} onClick={onNext} aria-label="下一页历史"><ChevronRight className="size-3.5" aria-hidden="true" /></Button></span></TooltipTrigger>
            <TooltipContent>{"下一页历史"}</TooltipContent>
          </Tooltip>
        </div>
      ) : null}
    </div>
  )
}

function ChangeFilterToolbar({ filters, filterOptions, onFilterChange, refreshing = false, refreshFailure = "", refreshLabel = "待处理分组事实", onRefresh, disabled = false }: {
  filters: UpstreamGroupChangeFilters
  filterOptions: UpstreamGroupChangeFilterOptions
  onFilterChange: (key: keyof UpstreamGroupChangeFilters, value: string) => void
  refreshing?: boolean
  refreshFailure?: string
  refreshLabel?: string
  onRefresh?: () => void
  disabled?: boolean
}) {
  return (
    <div className="space-y-2 border-b border-border bg-muted/10 px-4 py-3 sm:px-6">
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative min-w-56 flex-1">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" aria-hidden="true" />
          <Input value={filters.search} onChange={(event) => onFilterChange("search", event.target.value)} placeholder="搜索渠道、分组、来源或判定原因" aria-label="搜索渠道、分组、来源或判定原因" className="pl-8" disabled={disabled} />
        </div>
        {onRefresh ? (
          <Tooltip>
            <TooltipTrigger asChild>
              <Button type="button" size="icon-sm" variant="outline" disabled={disabled || refreshing} aria-busy={refreshing} aria-label={`刷新${refreshLabel}`} onClick={onRefresh}>
                <RefreshCw className={refreshing ? "size-3.5 animate-spin" : "size-3.5"} aria-hidden="true" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>{refreshing ? `正在刷新${refreshLabel}` : `刷新${refreshLabel}`}</TooltipContent>
          </Tooltip>
        ) : null}
        {refreshFailure ? (
          <FailureDetailButton label="刷新待处理分组事实" reason={refreshFailure} />
        ) : null}
      </div>
      <div className="grid gap-2 sm:grid-cols-3">
        <FilterSelect label="筛选渠道" value={filters.channel} allLabel="全部渠道" options={filterOptions.channels} onChange={(value) => onFilterChange("channel", value)} disabled={disabled} />
        <FilterSelect label="筛选分组" value={filters.group} allLabel="全部分组" options={filterOptions.groups} onChange={(value) => onFilterChange("group", value)} disabled={disabled} />
        <FilterSelect label="筛选来源" value={filters.source} allLabel="全部来源" options={filterOptions.sources} onChange={(value) => onFilterChange("source", value)} disabled={disabled} />
      </div>
    </div>
  )
}

function FilterSelect({ label, value, allLabel, options, onChange, disabled = false }: { label: string; value: string; allLabel: string; options: { value: string; label: string }[]; onChange: (value: string) => void; disabled?: boolean }) {
  return (
    <Select value={value} onValueChange={onChange}>
      <SelectTrigger className="w-full bg-background" aria-label={label} title={label} disabled={disabled}><SelectValue /></SelectTrigger>
      <SelectContent>
        <SelectItem value={upstreamGroupChangeFilterAll}>{allLabel}</SelectItem>
        {options.map((option) => <SelectItem key={option.value} value={option.value}>{option.label}</SelectItem>)}
      </SelectContent>
    </Select>
  )
}

function ChangeStatusIndicator({ change }: { change: GatewayUpstreamGroupChange }) {
  if (upstreamGroupChangeRequiresManualRemovalReview(change)) return <StatusTooltip icon={AlertTriangle} label="托管 Key 待复核" detail={upstreamGroupChangeManualReviewReasonLabel(change)} className="text-warning" />
  if (change.status === "ignored") return <StatusTooltip icon={Ban} label="已忽略" detail={upstreamGroupChangeReasonLabel(change)} className="text-warning" />
  if (change.status === "assigned") return <StatusTooltip icon={CheckCircle2} label="已加入候选分组" detail={upstreamGroupChangeReasonLabel(change)} className="text-success" />
  if (change.status === "superseded") return <StatusTooltip icon={History} label="已过期" detail={upstreamGroupChangeReasonLabel(change)} className="text-muted-foreground" />
  if (change.status === "acknowledged" || change.kind === "removed") return <StatusTooltip icon={CheckCircle2} label="已确认移除" detail="上游移除变更已经处理" className="text-success" />
  if (change.assignment_state === "conflict") return <StatusTooltip icon={AlertTriangle} label="规则冲突" detail={upstreamGroupChangeReasonLabel(change)} className="text-warning" />
  if (change.assignment_state === "awaiting_facts") return <StatusTooltip icon={Clock3} label="等待事实" detail={upstreamGroupChangeReasonLabel(change)} className="text-warning" />
  if (change.assignment_state === "ready") return <StatusTooltip icon={CheckCircle2} label="可处理" detail={upstreamGroupChangeReasonLabel(change)} className="text-success" />
  return <StatusTooltip icon={CircleHelp} label="待人工处理" detail={upstreamGroupChangeReasonLabel(change)} className="text-muted-foreground" />
}

function StatusTooltip({ icon: Icon, label, detail, className }: { icon: typeof Inbox; label: string; detail: string; className?: string }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span role="img" aria-label={`${label}：${detail}`} className={`inline-flex size-5 shrink-0 cursor-help items-center justify-center rounded-full ${className ?? "text-muted-foreground"}`}>
          <Icon className="size-3.5" aria-hidden="true" />
        </span>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs"><span className="font-semibold">{label}</span><span>{`：${detail}`}</span></TooltipContent>
    </Tooltip>
  )
}

function EmptyState({ icon: Icon, label, compact = false, spinning = false }: { icon: typeof Inbox; label: string; compact?: boolean; spinning?: boolean }) {
  return (
    <div className={`flex flex-col items-center justify-center text-center text-sm text-muted-foreground ${compact ? "min-h-28" : "min-h-64"}`}>
      <Icon className={`mb-2 size-5 ${spinning ? "animate-spin" : ""}`} aria-hidden="true" />
      <span>{label}</span>
    </div>
  )
}

function LoadFailureState({ label, reason, retrying, onRetry }: { label: string; reason: string; retrying: boolean; onRetry: () => void }) {
  return (
    <div className="flex min-h-64 flex-col items-center justify-center gap-3 px-4 text-center">
      <AlertTriangle className="size-5 text-danger" aria-hidden="true" />
      <div className="text-sm font-medium">{label}</div>
      <div className="flex items-center gap-2">
        <FailureDetailButton label={label} reason={reason || label} />
        <Button type="button" size="sm" variant="outline" disabled={retrying} aria-busy={retrying} onClick={onRetry}>
          <RefreshCw className={retrying ? "size-3.5 animate-spin" : "size-3.5"} aria-hidden="true" />
          {"重试"}
        </Button>
      </div>
    </div>
  )
}

function FailureDetailButton({ label, reason }: { label: string; reason: string }) {
  return (
    <Dialog>
      <DialogTrigger asChild>
        <button type="button" className="inline-flex h-6 shrink-0 items-center rounded-md border border-danger/25 bg-danger/5 px-2 text-[10px] font-medium text-danger hover:bg-danger/10" aria-label={`查看 ${label} 的失败详情`}>
          {"异常"}
        </button>
      </DialogTrigger>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>{`${label} · 异常详情`}</DialogTitle>
        </DialogHeader>
        <pre className="max-h-[min(28rem,calc(100vh-12rem))] overflow-auto whitespace-pre-wrap break-words rounded-md border border-border bg-muted/20 p-3 text-xs text-muted-foreground">{reason}</pre>
        <div className="flex justify-end">
          <Button type="button" size="sm" variant="outline" onClick={() => { void copyBatchFailureReason(reason) }}>
            <Copy className="size-3.5" aria-hidden="true" />
            {"复制详情"}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}

function hasActiveFilters(filters: UpstreamGroupChangeFilters) {
  return Boolean(filters.search.trim()) || filters.channel !== upstreamGroupChangeFilterAll || filters.group !== upstreamGroupChangeFilterAll || filters.source !== upstreamGroupChangeFilterAll
}

async function copyBatchFailureReason(reason: string) {
  try {
    await navigator.clipboard.writeText(reason)
    toast.success("失败详情已复制")
  } catch {
    toast.error("复制失败")
  }
}

function formatRatioWithUnit(value: number | null | undefined) {
  const formatted = formatRatio(value)
  return formatted === "—" ? formatted : `${formatted}x`
}

function upstreamGroupChangeRatioTitle(change: GatewayUpstreamGroupChange) {
  return `转换后输入倍率 ${formatRatioWithUnit(change.ratio)} · 转换后输出倍率 ${formatRatioWithUnit(change.completion_ratio)}`
}


function historyStatusLabel(change: GatewayUpstreamGroupChange) {
  if (change.status === "assigned") return "已加入分组"
  if (change.status === "ignored") return "已忽略"
  if (change.status === "superseded") return "已过期"
  if (change.kind === "removed") return "已自动清理"
  return "已确认"
}

function historyActorLabel(change: GatewayUpstreamGroupChange) {
  return change.removal_manual_review_resolved_by
    || change.removal_manual_review_actor
    || change.assignment_audit_actor
    || "系统"
}

function historyTargetsLabel(change: GatewayUpstreamGroupChange, candidateGroupByID: Map<number, GatewayCandidateGroup>) {
  const targetIDs = change.assignment_audit_target_candidate_group_ids ?? []
  if (targetIDs.length > 0) return candidateGroupNames(targetIDs, candidateGroupByID)
  if (change.removal_manual_review_resolution) return change.removal_manual_review_resolution
  if (change.kind === "removed") return "上游分组移除"
  if (change.assignment_audit_action === "manual_ignore") return "已忽略，不加入候选分组"
  return "未记录目标"
}

function HistoryDetailButton({ change, candidateGroupByID }: { change: GatewayUpstreamGroupChange; candidateGroupByID: Map<number, GatewayCandidateGroup> }) {
  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button type="button" size="icon-sm" variant="ghost" className="size-7" aria-label={`查看 ${change.group_name} 处理详情`}>
          <CircleHelp className="size-3.5" aria-hidden="true" />
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>{`${upstreamGroupChangeChannelLabel(change)} / ${change.group_name} · 处理详情`}</DialogTitle>
        </DialogHeader>
        <dl className="grid grid-cols-[7rem_minmax(0,1fr)] gap-x-3 gap-y-2 rounded-md border border-border bg-muted/10 p-3 text-xs">
          <dt className="text-muted-foreground">{"处理结果"}</dt><dd className="font-medium">{historyStatusLabel(change)}</dd>
          <dt className="text-muted-foreground">{"操作人"}</dt><dd className="break-words">{historyActorLabel(change)}</dd>
          <dt className="text-muted-foreground">{"目标 / 处理说明"}</dt><dd className="whitespace-pre-wrap break-words">{historyTargetsLabel(change, candidateGroupByID)}</dd>
          <dt className="text-muted-foreground">{"操作"}</dt><dd className="break-words">{change.assignment_audit_action || (change.kind === "removed" ? "managed_key_removal" : "-")}</dd>
          <dt className="text-muted-foreground">{"触发来源"}</dt><dd className="break-words">{change.assignment_audit_trigger || (change.removal_manual_review_actor ? "manual_review" : "-")}</dd>
          <dt className="text-muted-foreground">{"代次"}</dt><dd className="tabular-nums">{change.assignment_audit_presence_generation || change.presence_generation || "-"}</dd>
          <dt className="text-muted-foreground">{"事实版本"}</dt><dd className="break-words">{change.facts_digest || "-"}</dd>
          <dt className="text-muted-foreground">{"处理时间"}</dt><dd>{dateTime(change.resolved_at ?? change.assignment_audit_created_at ?? change.detected_at)}</dd>
        </dl>
        {change.removal_manual_review_reason ? (
          <div className="rounded-md border border-warning/25 bg-warning/5 p-3 text-xs">
            <div className="font-medium">{"复核原因"}</div>
            <p className="mt-1 whitespace-pre-wrap break-words text-muted-foreground">{change.removal_manual_review_reason}</p>
          </div>
        ) : null}
      </DialogContent>
    </Dialog>
  )
}

function suggestedTargetsLabel(change: GatewayUpstreamGroupChange, candidateGroupByID: Map<number, GatewayCandidateGroup>) {
  const ids = change.suggested_candidate_group_ids ?? []
  if (ids.length === 0) return "暂无"
  return ids.map((id) => candidateGroupByID.get(id)?.name ?? `候选分组 #${id}`).join("、")
}

function candidateGroupNames(ids: number[], candidateGroupByID: Map<number, GatewayCandidateGroup>) {
  if (ids.length === 0) return "未选择候选分组"
  return ids.map((id) => candidateGroupByID.get(id)?.name ?? `候选分组 #${id}`).join("、")
}
