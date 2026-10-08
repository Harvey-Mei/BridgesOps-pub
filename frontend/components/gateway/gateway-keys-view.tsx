import { HelpHint } from "@/components/ui/help-hint"
import { useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from "react"
import {
  Check,
  ChevronDown,
  Copy,
  Eye,
  EyeOff,
  ExternalLink,
  FlaskConical,
  KeyRound,
  Layers3,
  MoreHorizontal,
  Pencil,
  Pin,
  Plus,
  RefreshCw,
  Search,
  Save,
  SlidersHorizontal,
  Trash2,
  X,
} from "lucide-react"
import type {
  GatewayApp,
  GatewayCandidateGroup,
  GatewayDownstreamKey,
  GatewayDownstreamKeyCreateResult,
  GatewayKeyGroup,
  GatewayManualMappedTargetPreview,
  GatewayModelFamily,
  GatewayModelMappingStrategy,
  GatewayRouteCandidate,
} from "@/lib/api-types"
import { useGatewayCandidateGroupPreview, useGatewayManualMappedTargetPreview, useGatewayManualRoutePreview, type GatewayCandidatePreviewModelContext, type GatewayKeyRoutePreviewState } from "@/lib/queries"
import { compactCount, compactTokenCount, dateTime, money, relativeTime } from "@/lib/format"
import { candidateSelectionForKey } from "@/lib/gateway-key-route-policy"
import {
  modelMappingDraftIsDirty,
  modelMappingDraftIsValid,
  modelMappingDraftCandidateGroupCompatible,
  modelMappingStateFromKey,
  normalizeModelMappingStrategy,
  setModelMappingEnabled,
  type ModelMappingDraft,
} from "@/lib/model-mapping-editor"
import { cn } from "@/lib/utils"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { PageHeader } from "@/components/ui/page-header"
import { Input } from "@/components/ui/input"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { CandidateGroupsDialog } from "@/components/gateway/candidate-groups-dialog"
import { GatewayKeyGroupsDialog } from "@/components/gateway/gateway-key-groups-dialog"
import { ModelFamilyMark } from "@/components/model-family-mark"
import { modelFamilyLabel } from "@/lib/model-families"
import { officialReasoningEffortLabel } from "@/lib/reasoning-effort"
import {
  clientImportCandidateRoutable,
  clientImportDefaultModel,
  clientImportFixedDefaultModel,
} from "@/components/gateway/client-import-data"
import type { GatewayRoutingEnableCandidate } from "@/lib/gateway-routing-enable"
import {
  readCandidatePreviewModelPreference,
  resolveCandidatePreviewModelPreference,
  writeCandidatePreviewModelPreference,
} from "@/lib/candidate-preview-preference"
import {
  candidateAvailabilityLabel,
  candidateAvailabilityReasons,
  manualMappedTargetSelectionState,
  manualRouteCandidateSelectable,
} from "@/lib/route-candidate-availability"
import {
  candidateManagementInventoryCandidateIsCurrent,
  candidateManagementInventoryCandidates,
  candidateManagementMemberKey,
  candidateManagementRankedMemberKeys,
  type CandidateManagementInventoryCandidate,
} from "@/lib/candidate-management-inventory"
import {
  buildCandidateGroupMembershipRows,
  candidateGroupMembershipFilterAll,
  candidateGroupMembershipProtocolOptions,
  candidateGroupMembershipRowState,
  filterCandidateGroupMembershipRows,
  type CandidateGroupMembershipRow,
} from "@/lib/candidate-group-membership"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

type RoutePreviewState = GatewayKeyRoutePreviewState

type GatewayKeysViewProps = {
  keys: GatewayDownstreamKey[]
  loading: boolean
  refreshing: boolean
  error: string | null
  showCreate: boolean
  listHeaderActions: ReactNode
  name: string
  app: GatewayApp
  modelFamily: GatewayModelFamily
  candidateGroupID: string
  keyGroupID: string
  candidateGroups: GatewayCandidateGroup[]
  keyGroups: GatewayKeyGroup[]
  candidateGroupsLoading: boolean
  candidateInventoryLoading: boolean
  groupCandidates: GatewayRouteCandidate[]
  creating: boolean
  created: GatewayDownstreamKeyCreateResult | null
  visibleKeyIDs: Record<string, boolean>
  editingID: number | null
  editingName: string
  savingKeyIDs: ReadonlySet<number>
  testingID: number | null
  keyRoutePreviews: Record<number, GatewayKeyRoutePreviewState>
  onToggleCreate: () => void
  onRefresh: () => void
  onCopy: (value: string, label?: string) => void
  onCreate: (event: FormEvent<HTMLFormElement>) => void
  onCancelCreate: () => void
  onNameChange: (value: string) => void
  onAppChange: (value: GatewayApp) => void
  onModelFamilyChange: (value: GatewayModelFamily) => void
  onCandidateGroupChange: (value: string) => void
  onKeyGroupChange: (value: string) => void
  onCandidateGroupsChanged: () => void
  onEnableRoutingGroup: (candidate: GatewayRoutingEnableCandidate) => void
  isRoutingGroupEnableSaving: (candidate: GatewayRoutingEnableCandidate) => boolean
  onKeyGroupsChanged: () => void
  onToggleKeyVisible: (id: string) => void
  onImportCCSwitch: (apiKey: string, keyName: string, app: GatewayApp, contextLabel: string, modelName?: string) => void
  renderClientConfigPopover: (apiKey: string, app: GatewayApp, modelName?: string, disabled?: boolean, buttonClassName?: string) => ReactNode
  renderRouteCandidateItem: (
    key: GatewayDownstreamKey,
    candidate: GatewayRouteCandidate,
    rank: number,
    onChanged?: (disabled: boolean) => void,
    mutationsDisabled?: boolean,
  ) => ReactNode
  onEditingNameChange: (value: string) => void
  onToggleSmartRouting: (key: GatewayDownstreamKey, checked: boolean, availableCandidates: GatewayRouteCandidate[]) => void
  onSetManualRoute: (key: GatewayDownstreamKey, candidate: GatewayRouteCandidate) => void
  onToggleEnabled: (id: number, checked: boolean) => void
  onStartEdit: (key: GatewayDownstreamKey) => void
  onSaveEdit: (id: number, keyGroupID: string, candidateGroupID: string) => void
  onSaveModelMapping: (id: number, draft: ModelMappingDraft) => void
  onCancelEdit: () => void
  onDeleteKey: (id: number, name: string) => void
  onTestKey: (key: GatewayDownstreamKey) => void
}

export function GatewayKeysView({
  keys,
  loading,
  refreshing,
  error,
  showCreate,
  listHeaderActions,
  name,
  app,
  modelFamily,
  candidateGroupID,
  keyGroupID,
  candidateGroups,
  keyGroups,
  candidateGroupsLoading,
  candidateInventoryLoading,
  groupCandidates,
  creating,
  created,
  visibleKeyIDs,
  editingID,
  editingName,
  savingKeyIDs,
  testingID,
  keyRoutePreviews,
  onToggleCreate,
  onRefresh,
  onCopy,
  onCreate,
  onCancelCreate,
  onNameChange,
  onAppChange,
  onModelFamilyChange,
  onCandidateGroupChange,
  onKeyGroupChange,
  onCandidateGroupsChanged,
  onEnableRoutingGroup,
  isRoutingGroupEnableSaving,
  onKeyGroupsChanged,
  onToggleKeyVisible,
  onImportCCSwitch,
  renderClientConfigPopover,
  renderRouteCandidateItem,
  onEditingNameChange,
  onToggleSmartRouting,
  onSetManualRoute,
  onToggleEnabled,
  onStartEdit,
  onSaveEdit,
  onSaveModelMapping,
  onCancelEdit,
  onDeleteKey,
  onTestKey,
}: GatewayKeysViewProps) {
  const [search, setSearch] = useState("")
  const [statusFilter, setStatusFilter] = useState<"all" | "enabled" | "disabled" | "issues">("all")
  const [groupFilter, setGroupFilter] = useState("all")
  const [showManagement, setShowManagement] = useState(false)
  const [keyActionsOpenID, setKeyActionsOpenID] = useState<number | null>(null)
  const [editingKeyGroupID, setEditingKeyGroupID] = useState("__none__")
  const [editingCandidateGroupID, setEditingCandidateGroupID] = useState("__all__")
  const [manualRoutePickerKeyID, setManualRoutePickerKeyID] = useState<number | null>(null)
  const [candidateManagementKeyID, setCandidateManagementKeyID] = useState<number | null>(null)
  const enabledCount = keys.filter((key) => key.enabled).length
  const routeUnavailableIDs = useMemo(() => new Set(keys.filter((key) => {
    const preview = keyRoutePreviews[key.id] ?? pendingRoutePreview
    const candidates = routeCandidatesForKey(key, safeRouteCandidates(preview.data))
    const availableCandidates = enabledRouteCandidates(candidates)
    const selectedRoute = selectedManualCandidate(key, candidates)
			const selectedMappedTargetState = selectedRoute
			  ? manualMappedTargetSelectionState(selectedRoute, preview.mappedTargetData)
			  : null
			const selectedRouteDisabled = Boolean(selectedRoute && (
			  preview.mappedTargetLoading
			  || preview.mappedTargetError
			  || preview.mappedTargetData == null
			  || !selectedMappedTargetState?.selectable
			))
    return Boolean(preview.error) || (key.smart_routing
      ? !preview.loading && availableCandidates.length === 0
      : !selectedRoute || selectedRouteDisabled || Boolean(key.manual_route_invalid))
  }).map(key => key.id)), [keys, keyRoutePreviews])
  const editingKey = keys.find(key => key.id === editingID) ?? null
  const editSaving = editingKey !== null && savingKeyIDs.has(editingKey.id)
  const createdRoutePreview = created
    ? keyRoutePreviews[created.key.id] ?? pendingRoutePreview
    : null
  const createdRouteCandidates = created
    ? routeCandidatesForKey(created.key, safeRouteCandidates(createdRoutePreview?.data))
    : []
  const createdAvailableRouteCandidates = enabledRouteCandidates(createdRouteCandidates)
  const createdSelectedRoute = created && !created.key.smart_routing
    ? selectedManualCandidate(created.key, createdRouteCandidates)
    : null
  const createdRouteCandidate = created?.key.smart_routing
    ? createdAvailableRouteCandidates[0]
    : createdSelectedRoute && manualRouteCandidateRoutable(createdSelectedRoute) ? createdSelectedRoute : null
  const createdClientImportCandidates = created?.key.smart_routing
    ? createdAvailableRouteCandidates
    : createdRouteCandidate ? [createdRouteCandidate] : []
  const createdRouteUnavailable = created == null || Boolean(createdRoutePreview?.loading) || Boolean(createdRoutePreview?.error)
    || (created.key.smart_routing
      ? createdAvailableRouteCandidates.length === 0
      : !createdRouteCandidate || Boolean(created.key.manual_route_invalid))
  const createdClientImportDisabled = createdRouteUnavailable
  const createdClientModelName = created
    ? clientImportModelName(created.key, createdRouteCandidate, createdClientImportCandidates)
    : ""
  const manualRoutePickerKey = keys.find((key) => key.id === manualRoutePickerKeyID) ?? null
  const manualRoutePickerPreview = useGatewayManualRoutePreview(manualRoutePickerKey?.id ?? null, manualRoutePickerKey != null)
  const manualMappedTargetPreview = useGatewayManualMappedTargetPreview(manualRoutePickerKey?.id ?? null, manualRoutePickerKey != null)
  const manualRoutePickerCandidates = manualRoutePickerKey
    ? manualCandidatesForKey(manualRoutePickerKey, safeRouteCandidates(manualRoutePickerPreview.data))
    : []
  const manualRoutePickerSelected = manualRoutePickerKey && !manualRoutePickerKey.smart_routing
    ? selectedManualCandidate(manualRoutePickerKey, manualRoutePickerCandidates)
    : null
  const candidateManagementKey = keys.find((key) => key.id === candidateManagementKeyID) ?? null
  const normalizedSearch = search.trim().toLowerCase()
  const visibleKeys = useMemo(() => {
    const filtered = keys.filter((key) => {
      if (statusFilter === "enabled" && !key.enabled) return false
      if (statusFilter === "disabled" && key.enabled) return false
      if (statusFilter === "issues" && !routeUnavailableIDs.has(key.id)) return false
      if (groupFilter !== "all" && String(key.key_group_id || "none") !== groupFilter) return false
      const haystack = [
        key.name,
        key.key_prefix,
        key.app,
        key.key_group_name ?? "",
        candidateGroupLabel(key, candidateGroups),
        modelFamilyLabel(key.model_family ?? key.app),
      ].join(" ").toLowerCase()
      return haystack.includes(normalizedSearch)
    })
    return [...filtered].sort((a, b) => {
    const groupCompare = (a.key_group_name || "未分组").localeCompare(b.key_group_name || "未分组", "zh-CN")
    return groupCompare || a.name.localeCompare(b.name, "zh-CN")
    })
  }, [keys, normalizedSearch, statusFilter, groupFilter, routeUnavailableIDs, candidateGroups])

  function startEditingKey(key: GatewayDownstreamKey) {
    setEditingKeyGroupID(key.key_group_id ? String(key.key_group_id) : "__none__")
	setEditingCandidateGroupID(candidateSelectionForKey(key))
    onStartEdit(key)
  }

  function cancelEditingKey() {
    setEditingKeyGroupID("__none__")
    setEditingCandidateGroupID("__all__")
    onCancelEdit()
  }

  function changeEditingCandidateGroup(value: string) {
    setEditingCandidateGroupID(value)
  }

  return (
    <section className="space-y-4">
      <PageHeader icon={KeyRound} title="密钥管理" description="管理密钥、候选范围和路由方式" actions={<>
        <Button variant="ghost" size="icon-sm" aria-label="刷新密钥列表" title="刷新" disabled={loading || refreshing} onClick={onRefresh}><RefreshCw className={cn("size-4", (loading || refreshing) && "animate-spin")} /></Button>
        <Button variant="outline" onClick={() => setShowManagement(value => !value)} aria-expanded={showManagement} aria-controls="key-management-tools"><SlidersHorizontal className="size-4" />管理与接入</Button>
        <Button onClick={onToggleCreate}><Plus className="size-4" />创建 Key</Button>
      </>} />
      <div id="key-management-tools" hidden={!showManagement} className="rounded-xl border bg-card px-4 py-3">
        <div className="flex flex-wrap items-center gap-2">
          <GatewayKeyGroupsDialog groups={keyGroups} keys={keys} candidateGroups={candidateGroups} onChanged={onKeyGroupsChanged} />
          <CandidateGroupsDialog groups={candidateGroups} candidates={groupCandidates} loading={candidateGroupsLoading} onChanged={onCandidateGroupsChanged} onEnableRoutingGroup={onEnableRoutingGroup} isRoutingGroupEnableSaving={isRoutingGroupEnableSaving} />
          <span className="mx-1 h-5 border-l" />
          {listHeaderActions}
        </div>
      </div>

      <Dialog open={showCreate} onOpenChange={(open) => { if (!open && !creating) onCancelCreate() }}>
        <DialogContent className="max-h-[calc(100vh-2rem)] overflow-y-auto sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>{created ? "Key 已创建" : "创建下游 Key"}</DialogTitle>
          </DialogHeader>
          {created ? (
            <div className="space-y-4">
              <div className="space-y-3 rounded-lg border border-emerald-200 bg-emerald-50 p-4 text-emerald-950 dark:border-emerald-900/60 dark:bg-emerald-950/30 dark:text-emerald-100">
                <div className="flex items-center gap-2 text-sm font-medium">
                  <Check className="size-4" />
                  <span>{created.key.name}</span>
                  <ModelFamilyMark family={created.key.model_family ?? created.key.app} />
                </div>
                <div className="flex min-w-0 gap-1">
                  <Input
                    readOnly
                    value={visibleKeyIDs[`created-${created.key.id}`] ? created.plain : maskAPIKey(created.plain)}
                    className="h-9 min-w-0 bg-white/80 font-mono text-xs text-emerald-950 dark:bg-emerald-950 dark:text-emerald-100"
                  />
                  <Button
                    type="button"
                    size="icon-sm"
                    variant="outline"
                    onClick={() => onToggleKeyVisible(`created-${created.key.id}`)}
                    aria-label={visibleKeyIDs[`created-${created.key.id}`] ? "隐藏 key" : "显示 key"}
                    className="shrink-0 border-emerald-300 bg-white/80 text-emerald-950 hover:bg-white dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-100"
                  >
                    {visibleKeyIDs[`created-${created.key.id}`] ? <EyeOff className="size-3.5" /> : <Eye className="size-3.5" />}
                  </Button>
                  <Button
                    type="button"
                    size="icon-sm"
                    variant="outline"
                    onClick={() => onCopy(created.plain, "key")}
                    aria-label="复制 key"
                    className="shrink-0 border-emerald-300 bg-white/80 text-emerald-950 hover:bg-white dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-100"
                  >
                    <Copy className="size-3.5" />
                  </Button>
                </div>
                <div className="flex flex-wrap gap-2">
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    disabled={createdClientImportDisabled}
                    title={createdClientImportDisabled ? "正在等待当前 Key 的可路由候选，暂不能生成客户端配置" : undefined}
                    onClick={() => onImportCCSwitch(
                      created.plain,
                      ccSwitchProviderName(created.key, keyGroups),
                      created.key.app,
                      clientImportContextLabel(created.key, keyGroups),
                      createdClientModelName,
                    )}
                    className="border-emerald-300 bg-white/80 text-emerald-950 hover:bg-white dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-100"
                  >
                    <ExternalLink className="size-3.5" />
                    {"导入客户端"}
                  </Button>
                  {renderClientConfigPopover(
                    created.plain,
                    created.key.app,
                    createdClientModelName,
                    createdClientImportDisabled,
                    "border-emerald-300 bg-white/80 text-emerald-950 hover:bg-white dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-100",
                  )}
                </div>
              </div>
              <div className="flex justify-end">
                <Button type="button" onClick={onCancelCreate}>{"完成"}</Button>
              </div>
            </div>
          ) : (
            <form className="space-y-5" onSubmit={onCreate}>
              <div className="grid gap-4 sm:grid-cols-2">
                <label className="space-y-1.5 sm:col-span-2">
                  <span className="text-xs font-medium text-muted-foreground">{"名称"}</span>
                  <Input
                    id="gateway-key-name"
                    value={name}
                    onChange={(event) => onNameChange(event.target.value)}
                    placeholder="default"
                    disabled={creating}
                    autoFocus
                  />
                </label>
                <label className="space-y-1.5">
                  <span className="text-xs font-medium text-muted-foreground">{"Key 分组"}</span>
                  <Select value={keyGroupID} onValueChange={onKeyGroupChange} disabled={creating}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="__none__">{"未分组"}</SelectItem>
                      {keyGroups.map((group) => <SelectItem key={group.id} value={String(group.id)}>{group.name}</SelectItem>)}
                    </SelectContent>
                  </Select>
                </label>
                <label className="space-y-1.5">
                  <span className="text-xs font-medium text-muted-foreground">{"候选分组"}</span>
                  <Select value={candidateGroupID} onValueChange={onCandidateGroupChange} disabled={creating || candidateGroupsLoading}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {keyGroupID !== "__none__" ? <SelectItem value="__policy__">{"按 Key 分组默认策略"}</SelectItem> : null}
                      <SelectItem value="__all__">{"明确使用全部候选"}</SelectItem>
                      {candidateGroups.map((group) => <SelectItem key={group.id} value={String(group.id)}>{`${group.name} · ${group.members?.length ?? 0} 个`}</SelectItem>)}
                    </SelectContent>
                  </Select>
                </label>
                <label className="space-y-1.5">
                  <span className="text-xs font-medium text-muted-foreground">{"协议"}</span>
                  <Select value={app} onValueChange={(value) => onAppChange(value as GatewayApp)} disabled={creating}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="codex">{"OpenAI API 协议"}</SelectItem>
                      <SelectItem value="claude">{"Claude"}</SelectItem>
                      <SelectItem value="gemini">{"Gemini"}</SelectItem>
                    </SelectContent>
                  </Select>
                </label>
                <label className="space-y-1.5">
                  <span className="text-xs font-medium text-muted-foreground">{"模型家族"}</span>
                  <Select value={modelFamily} onValueChange={(value) => onModelFamilyChange(value as GatewayModelFamily)} disabled={creating}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent className="max-h-72">
                      {app === "codex" ? (
                        <>
                          <SelectItem value="codex">{"GPT / Codex"}</SelectItem>
                          <SelectItem value="grok">{"Grok"}</SelectItem>
                          <SelectItem value="deepseek">{"DeepSeek"}</SelectItem>
                          <SelectItem value="qwen">{"Qwen"}</SelectItem>
                          <SelectItem value="mistral">{"Mistral"}</SelectItem>
                          <SelectItem value="llama">{"Llama"}</SelectItem>
                          <SelectItem value="glm">{"GLM"}</SelectItem>
                          <SelectItem value="minimax">{"MiniMax"}</SelectItem>
                          <SelectItem value="moonshot">{"Moonshot"}</SelectItem>
                          <SelectItem value="kimi">{"Kimi"}</SelectItem>
                          <SelectItem value="yi">{"Yi"}</SelectItem>
                          <SelectItem value="ernie">{"ERNIE"}</SelectItem>
                        </>
                      ) : (
                        <SelectItem value={app}>{app === "claude" ? "Claude" : "Gemini"}</SelectItem>
                      )}
                    </SelectContent>
                  </Select>
                </label>
              </div>
              <div className="flex justify-end gap-1 border-t border-border pt-4">
                <Button type="button" variant="outline" disabled={creating} onClick={onCancelCreate}>
                  <X className="size-4" />
                  {"取消"}
                </Button>
                <Button type="submit" disabled={creating}>
                  <Plus className="size-4" />
                  {creating ? "创建中" : "创建"}
                </Button>
              </div>
            </form>
          )}
        </DialogContent>
      </Dialog>

      <Dialog open={editingKey !== null} onOpenChange={open => { if (!open && !editSaving) cancelEditingKey() }}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader><DialogTitle>编辑密钥</DialogTitle></DialogHeader>
          {editingKey ? <form className="space-y-4" onSubmit={event => { event.preventDefault(); onSaveEdit(editingKey.id, editingKeyGroupID, editingCandidateGroupID) }}>
            <label className="grid gap-2 text-sm font-medium">名称<Input value={editingName} onChange={event => onEditingNameChange(event.target.value)} disabled={editSaving} autoFocus required /></label>
            <div className="grid grid-cols-2 gap-4">
              <label className="grid gap-2 text-sm font-medium">Key 分组
                <Select value={editingKeyGroupID} onValueChange={setEditingKeyGroupID} disabled={editSaving}><SelectTrigger aria-label="Key 分组"><SelectValue /></SelectTrigger><SelectContent><SelectItem value="__none__">未分组</SelectItem>{keyGroups.map(group => <SelectItem key={group.id} value={String(group.id)}>{group.name}</SelectItem>)}</SelectContent></Select>
              </label>
              <label className="grid gap-2 text-sm font-medium">候选范围
                <Select value={editingCandidateGroupID} onValueChange={changeEditingCandidateGroup} disabled={editSaving || candidateGroupsLoading}><SelectTrigger aria-label={`${editingKey.name} 候选分组`}><SelectValue /></SelectTrigger><SelectContent>{editingKeyGroupID !== "__none__" ? <SelectItem value="__policy__">按 Key 分组默认策略</SelectItem> : null}<SelectItem value="__all__">全部候选</SelectItem>{candidateGroups.map(group => <SelectItem key={group.id} value={String(group.id)}>{group.name}</SelectItem>)}</SelectContent></Select>
              </label>
            </div>
            <DialogFooter><Button type="button" variant="outline" disabled={editSaving} onClick={cancelEditingKey}>取消</Button><Button type="submit" disabled={editSaving || !editingName.trim()}>{editSaving ? "保存中…" : "保存修改"}</Button></DialogFooter>
          </form> : null}
        </DialogContent>
      </Dialog>

      <div className="overflow-hidden rounded-xl border border-border bg-card">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b px-4 py-3">
          <div className="flex items-center gap-1" aria-label="密钥状态筛选">
            <KeyStat label="全部" value={String(keys.length)} hint="全部密钥" active={statusFilter === "all"} onClick={() => setStatusFilter("all")} />
            <KeyStat label="启用" value={String(enabledCount)} hint="已启用的密钥" active={statusFilter === "enabled"} onClick={() => setStatusFilter("enabled")} />
            <KeyStat label="停用" value={String(keys.length - enabledCount)} hint="已停用的密钥" active={statusFilter === "disabled"} onClick={() => setStatusFilter("disabled")} />
            <KeyStat label="路由异常" value={String(routeUnavailableIDs.size)} hint="查看路由需要处理的密钥" active={statusFilter === "issues"} onClick={() => setStatusFilter("issues")} warning={routeUnavailableIDs.size > 0} />
          </div>
          <div className="flex items-center gap-2">
            <Select value={groupFilter} onValueChange={setGroupFilter}><SelectTrigger className="h-9 w-36 bg-background text-sm" aria-label="筛选 Key 分组"><SelectValue /></SelectTrigger><SelectContent><SelectItem value="all">全部分组</SelectItem><SelectItem value="none">未分组</SelectItem>{keyGroups.map(group => <SelectItem key={group.id} value={String(group.id)}>{group.name}</SelectItem>)}</SelectContent></Select>
            <div className="relative w-60"><Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" /><Input value={search} onChange={event => setSearch(event.target.value)} placeholder="搜索密钥、分组…" aria-label="搜索下游 Key" className="h-9 bg-background pl-9" /></div>
          </div>
        </div>
        <div className="overflow-x-auto">
          <Table className="min-w-[960px] table-fixed">
            <colgroup><col className="w-[28%]" /><col className="w-[13%]" /><col className="w-[16%]" /><col className="w-[12%]" /><col className="w-[11%]" /><col className="w-[9%]" /><col className="w-[11%]" /></colgroup>
            <TableHeader><TableRow className="bg-muted/25 hover:bg-muted/25">
              <TableHead className="pl-5">密钥</TableHead><TableHead>路由</TableHead><TableHead>候选管理</TableHead><TableHead>模型映射</TableHead><TableHead className="text-right">费用</TableHead><TableHead className="text-center">状态</TableHead><TableHead className="pr-4 text-right">操作</TableHead>
            </TableRow></TableHeader>
            <TableBody>
              {visibleKeys.map((key) => {
                const keySaving = savingKeyIDs.has(key.id)
                const routePreview = keyRoutePreviews[key.id] ?? pendingRoutePreview
                const routeCandidates = routeCandidatesForKey(key, safeRouteCandidates(routePreview.data))
				const availableRouteCandidates = enabledRouteCandidates(routeCandidates)
	                const selectedRoute = selectedManualCandidate(key, routeCandidates)
		                const selectedMappedTargetState = selectedRoute
		                  ? manualMappedTargetSelectionState(selectedRoute, routePreview.mappedTargetData)
		                  : null
		                const selectedRouteBlocker = selectedMappedTargetState?.blockingReasons.map(candidateAvailabilityLabel).join("、") ?? ""
		                const selectedRouteDisabled = Boolean(selectedRoute && (
		                  routePreview.mappedTargetLoading
		                  || routePreview.mappedTargetError
		                  || routePreview.mappedTargetData == null
		                  || !selectedMappedTargetState?.selectable
		                ))
	                const routeUnavailable = Boolean(routePreview.error) || (key.smart_routing
	                  ? !routePreview.loading && availableRouteCandidates.length === 0
	                  : !selectedRoute || selectedRouteDisabled || Boolean(key.manual_route_invalid))
		                const routeSummaryCandidate = key.smart_routing
		                  ? availableRouteCandidates[0]
		                  : selectedRoute && !selectedRouteDisabled ? selectedRoute : null
		                const clientImportCandidates = key.smart_routing
		                  ? availableRouteCandidates
		                  : routeSummaryCandidate ? [routeSummaryCandidate] : []
		                const clientModelName = clientImportModelName(key, routeSummaryCandidate, clientImportCandidates)
		                const clientImportDisabled = routePreview.loading || routeUnavailable
                const recentRoute = key.recent_route
                const usedChannelName = key.smart_routing
                  ? recentRoute ? groupCandidates.find(candidate => candidate.channel_id === recentRoute.channel_id)?.channel_name || recentRoute.channel_name || `渠道 #${recentRoute.channel_id}` : ""
                  : key.manual_channel_name || selectedRoute?.channel_name || (key.manual_channel_id ? `渠道 #${key.manual_channel_id}` : "")
                const usedChannelDetail = key.smart_routing
                  ? recentRoute ? `最近成功请求 · ${dateTime(recentRoute.created_at)} · ${usedChannelName} / ${recentRoute.group_name} · ${recentRoute.model_name}` : "暂无真实请求记录"
                  : `固定目标 · ${usedChannelName || "未设置"} / ${key.manual_group_name || "未设置"}`

	                return (
                  <TableRow key={key.id} className="group hover:bg-accent/25">
                    <TableCell className="py-2 pl-5 align-middle">
                      <div className="space-y-1.5">
                        <div className="flex min-w-0 items-center gap-2">
                          <ModelFamilyMark family={key.model_family ?? key.app} />
                          <button type="button" className="min-w-0 truncate text-left text-sm font-semibold hover:text-brand focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring" onClick={() => startEditingKey(key)} disabled={keySaving} title={`${key.name} · 创建 ${dateTime(key.created_at)} · 最近使用 ${dateTime(key.last_used_at)}（${relativeTime(key.last_used_at)}）`} aria-label={`编辑 ${key.name}`}>{key.name}</button>
                          {key.key_group_id ? <span className="max-w-24 shrink-0 truncate rounded px-1.5 py-0.5 text-xs" style={keyGroupTone(key)} title={key.key_group_name}>{key.key_group_name || "已分组"}</span> : null}
                        </div>
                        <div className="flex min-w-0 items-center gap-1 pl-6">
                          <span className="min-w-0 truncate font-mono text-xs text-muted-foreground" title={visibleKeyIDs[`key-${key.id}`] ? undefined : key.key_prefix}>{key.api_key ? visibleKeyIDs[`key-${key.id}`] ? key.api_key : maskAPIKey(key.api_key) : `${key.key_prefix} · 无明文`}</span>
                          {key.api_key ? <>
                            <Button size="icon-sm" variant="ghost" className="size-6 shrink-0 text-muted-foreground" onClick={() => onCopy(key.api_key ?? "", "Key")} aria-label={`复制 ${key.name} Key`} title="复制 Key"><Copy className="size-3.5" /></Button>
                            <Button size="icon-sm" variant="ghost" className="size-6 shrink-0 text-muted-foreground" onClick={() => onToggleKeyVisible(`key-${key.id}`)} aria-label={`${visibleKeyIDs[`key-${key.id}`] ? "隐藏" : "显示"} ${key.name} Key`} title={visibleKeyIDs[`key-${key.id}`] ? "隐藏 Key" : "显示 Key"}>{visibleKeyIDs[`key-${key.id}`] ? <EyeOff className="size-3.5" /> : <Eye className="size-3.5" />}</Button>
                          </> : null}
                        </div>
                      </div>
                    </TableCell>
                    <TableCell className="py-2 align-middle">
                      <div className="space-y-1">
                        <div className="flex h-6 items-center gap-2">
                          <Switch aria-label={`${key.name} 智能路由`} checked={key.smart_routing} disabled={keySaving} onCheckedChange={checked => { if (!checked) { setManualRoutePickerKeyID(key.id); return } onToggleSmartRouting(key, true, availableRouteCandidates) }} />
                          <span className="text-sm">{key.smart_routing ? "智能" : "固定"}</span>
                          <Button variant="ghost" size="icon-sm" className="size-6 shrink-0 text-muted-foreground" aria-label={`${key.name} 选择固定候选`} title="选择固定候选，保存后切为固定路由" disabled={keySaving} onClick={() => setManualRoutePickerKeyID(key.id)}><Pin className="size-3.5" /></Button>
                        </div>
                        {routeUnavailable ? <span className="block truncate text-xs text-warning" title={selectedRouteBlocker || routePreview.error || undefined}>{routePreview.mappedTargetLoading ? "核对中" : routePreview.mappedTargetError ? "核对失败" : "暂无路由"}</span> : null}
                      </div>
                    </TableCell>
                    <TableCell className="py-2 align-middle">
                      <Button type="button" variant="ghost" className="-ml-2 h-auto min-h-12 w-[calc(100%+0.5rem)] min-w-0 justify-between gap-1 px-2 py-1 font-normal hover:text-brand" aria-label={`候选管理 · ${key.name}`} title={`${usedChannelDetail} · ${candidateGroupLabel(key, candidateGroups)}`} onClick={() => key.candidate_group_id ? setCandidateManagementKeyID(key.id) : startEditingKey(key)} disabled={keySaving}>
                        <span className="min-w-0 space-y-1 text-left"><span className="flex min-w-0 items-center gap-1.5 text-sm">{usedChannelName ? <span className="shrink-0 text-xs text-muted-foreground">{key.smart_routing ? "最近" : "固定"}</span> : null}<span className="truncate">{usedChannelName || (key.smart_routing ? "尚无使用记录" : "未设置")}</span></span><span className="flex min-w-0 items-center gap-1 text-xs text-muted-foreground"><Layers3 className="size-3 shrink-0" /><span className="truncate">{candidateGroupLabel(key, candidateGroups)}</span>{key.candidate_group_id ? <span className="shrink-0">· {candidateGroupMemberCount(key, candidateGroups)}</span> : null}</span></span><ChevronDown className="size-3.5 shrink-0 text-muted-foreground" />
                      </Button>
                    </TableCell>
                    <TableCell className="py-2 align-middle"><ModelMappingEditor keyData={key} candidateGroups={candidateGroups} candidateGroupsLoading={candidateGroupsLoading} groupCandidates={groupCandidates} saving={keySaving} onSave={onSaveModelMapping} /></TableCell>
                    <TableCell className="py-2 text-right align-middle">
                      <div className="space-y-1.5 tabular-nums" title={`今日 ${compactCount(key.usage?.today_requests ?? 0)} 次 · 累计 ${compactCount(key.usage?.total_requests ?? 0)} 次 · ${compactTokenCount(key.usage?.total_tokens ?? 0)} Token`}>
                        <div className="flex items-baseline justify-end gap-1"><span className="text-xs text-muted-foreground">今日</span><span className="text-sm font-medium">{money(key.usage?.today_cost, { precise: true })}</span></div>
                        <div className="flex items-baseline justify-end gap-1 text-xs text-muted-foreground"><span>累计</span><span>{money(key.usage?.total_cost, { precise: true })}</span></div>
                      </div>
                    </TableCell>
                    <TableCell className="py-2 align-middle">
                      <div className="flex flex-col items-center gap-1.5">
                        <label className="flex items-center gap-1.5 text-xs"><Switch aria-label={`${key.name}启用状态`} checked={key.enabled} disabled={keySaving} onCheckedChange={checked => onToggleEnabled(key.id, checked)} /><span className={key.enabled ? "text-foreground" : "text-muted-foreground"}>{key.enabled ? "启用" : "停用"}</span></label>
                        <span className={cn("text-xs tabular-nums", key.current_concurrency > 0 ? "text-brand" : "text-muted-foreground")}>{key.current_concurrency > 0 ? `${compactCount(key.current_concurrency)} 并发` : "空闲"}</span>
                      </div>
                    </TableCell>
                    <TableCell className="py-2 pr-3 align-middle">
                      <div className="flex items-center justify-end">
                        <Button size="icon-sm" variant="ghost" className="size-7" disabled={keySaving || !key.api_key || clientImportDisabled} title="客户端配置" aria-label={`${key.name} 客户端配置`} onClick={() => onImportCCSwitch(key.api_key ?? "", ccSwitchProviderName(key, keyGroups), key.app, clientImportContextLabel(key, keyGroups), clientModelName)}><ExternalLink className="size-4" /></Button>
                        <Button size="icon-sm" variant="ghost" className="size-7" disabled={keySaving || testingID !== null || !key.api_key} title="连通性测试" aria-label={`${key.name} 连通性测试`} onClick={() => onTestKey(key)}><FlaskConical className="size-4" /></Button>
                        <Popover open={keyActionsOpenID === key.id} onOpenChange={open => setKeyActionsOpenID(open ? key.id : null)}>
                          <PopoverTrigger asChild><Button size="icon-sm" variant="ghost" className="size-7" aria-label={`${key.name} 更多操作`}><MoreHorizontal className="size-4" /></Button></PopoverTrigger>
                          <PopoverContent align="end" className="w-40 space-y-1 p-1.5">
                            <Button size="sm" variant="ghost" className="w-full justify-start" disabled={keySaving} onClick={() => { setKeyActionsOpenID(null); startEditingKey(key) }}><Pencil className="size-4" />编辑密钥</Button>
                            <Button size="sm" variant="ghost" className="w-full justify-start text-destructive" disabled={keySaving} onClick={() => { setKeyActionsOpenID(null); onDeleteKey(key.id, key.name) }}><Trash2 className="size-4" />删除密钥</Button>
                          </PopoverContent>
                        </Popover>
                      </div>
                    </TableCell>
                  </TableRow>
                )
              })}
              {!loading && visibleKeys.length === 0 ? <TableRow><TableCell colSpan={7} className="h-40 text-center"><p className="text-sm text-muted-foreground">{keys.length ? "没有符合条件的密钥" : "还没有密钥"}</p>{keys.length ? <Button variant="link" onClick={() => { setSearch(""); setStatusFilter("all"); setGroupFilter("all") }}>清除筛选</Button> : <Button variant="link" onClick={onToggleCreate}>创建第一个 Key</Button>}</TableCell></TableRow> : null}
            </TableBody>
          </Table>
        </div>
        <div className="flex items-center justify-between border-t bg-muted/10 px-5 py-2.5 text-xs text-muted-foreground"><span>{visibleKeys.length === keys.length ? `共 ${keys.length} 个密钥` : `显示 ${visibleKeys.length} / ${keys.length} 个密钥`}</span><span>{loading || refreshing ? "正在刷新…" : ""}</span></div>
        {error ? <p role="alert" className="border-t px-5 py-3 text-sm text-destructive">{error}</p> : null}
      </div>
      <ManualRoutePickerDialog
        keyData={manualRoutePickerKey}
        candidates={manualRoutePickerCandidates}
        selected={manualRoutePickerSelected}
        mappedTargetPreview={manualMappedTargetPreview.data}
        loading={manualRoutePickerPreview.loading || manualRoutePickerPreview.refreshing || manualMappedTargetPreview.loading || manualMappedTargetPreview.refreshing}
        error={manualRoutePickerPreview.error || manualMappedTargetPreview.error}
        saving={manualRoutePickerKey != null && savingKeyIDs.has(manualRoutePickerKey.id)}
        onClose={() => setManualRoutePickerKeyID(null)}
        onSelect={(key, candidate) => {
          setManualRoutePickerKeyID(null)
          onSetManualRoute(key, candidate)
        }}
      />
      <CandidateManagementDialog
        keyData={candidateManagementKey}
        candidateGroups={candidateGroups}
        candidateGroupsLoading={candidateGroupsLoading}
        candidateInventoryLoading={candidateInventoryLoading}
        groupCandidates={groupCandidates}
        renderRouteCandidateItem={renderRouteCandidateItem}
        onClose={() => setCandidateManagementKeyID(null)}
      />
    </section>
  )
}

function ModelMappingEditor({
  keyData,
  candidateGroups,
  candidateGroupsLoading,
  groupCandidates,
  saving,
  onSave,
}: {
  keyData: GatewayDownstreamKey
  candidateGroups: GatewayCandidateGroup[]
  candidateGroupsLoading: boolean
  groupCandidates: GatewayRouteCandidate[]
  saving: boolean
  onSave: (id: number, draft: ModelMappingDraft) => void
}) {
  const persistedStrategy = keyData.model_mapping_strategy
  const persistedModel = keyData.model_mapping_model
  const persistedReasoningEffort = keyData.model_mapping_reasoning_effort
  const persisted = useMemo(() => modelMappingStateFromKey({
    model_mapping_strategy: persistedStrategy,
    model_mapping_model: persistedModel,
    model_mapping_reasoning_effort: persistedReasoningEffort,
  }), [persistedModel, persistedReasoningEffort, persistedStrategy])
  const [draft, setDraft] = useState<ModelMappingDraft>(persisted.draft)
  const [mappingEditorOpen, setMappingEditorOpen] = useState(false)
  const candidateGroupID = keyData.candidate_group_id ? String(keyData.candidate_group_id) : "__all__"
  const mappingUnsupportedReason = candidateGroupMappingUnsupportedReason(keyData, candidateGroupID, candidateGroups, candidateGroupsLoading)
  const mappingSupported = mappingUnsupportedReason === null
  const automaticMappingSupported = mappingSupported && keyData.app !== "claude"
  const mappingCandidates = candidatesForCandidateGroup(
    groupCandidates,
    candidateGroupID,
    candidateGroups,
  )
  const mappingModels = mappingCandidateModels(mappingCandidates)
  const reasoningOptions = modelMappingReasoningOptions(keyData.app, draft.strategy, draft.reasoningEffort)
  const enabled = draft.strategy !== "off"
  const invalidStrategy = persisted.invalidStrategy
  const dirty = modelMappingDraftIsDirty(draft, persisted)
  const draftCandidateCompatible = modelMappingDraftCandidateGroupCompatible(
    keyData,
    draft,
    candidateGroupID,
    candidateGroups,
    groupCandidates,
  )
  const manualModelUnsupported = draft.strategy === "manual" && !draftCandidateCompatible
  const draftValid = !enabled || (mappingSupported && modelMappingDraftIsValid(draft) && draftCandidateCompatible)
  const invalidReasonID = `model-mapping-${keyData.id}-invalid-strategy`
  const unsupportedReasonID = `model-mapping-${keyData.id}-unsupported`
  const manualModelReasonID = `model-mapping-${keyData.id}-manual-model`
  const describedBy = [invalidStrategy ? invalidReasonID : "", mappingUnsupportedReason ? unsupportedReasonID : "", manualModelUnsupported ? manualModelReasonID : ""].filter(Boolean).join(" ") || undefined
  const preview = keyData.model_mapping_preview
  const mappingError = preview?.error?.trim() || ""
  const previewModel = mappingError
    ? preview?.label?.trim() || "映射未就绪"
    : preview?.model?.trim() || keyData.model_mapping_model?.trim() || "映射未就绪"
  const previewReasoning = mappingError
    ? ""
    : preview?.reasoning_effort?.trim() || keyData.model_mapping_reasoning_effort?.trim() || ""
  const previewDetail = mappingError
    ? [preview?.source?.trim(), preview?.label?.trim(), mappingError].filter(Boolean).join(" · ")
    : [preview?.source?.trim(), previewModel, previewReasoning || "Radar 自动"].filter(Boolean).join(" · ")

  useEffect(() => {
    setDraft(persisted.draft)
  }, [keyData.id, persisted])

  function changeStrategy(value: string) {
    const strategy = normalizeModelMappingStrategy(value)
    setDraft((current) => ({
      strategy,
      model: strategy === "manual" ? current.model : "",
      reasoningEffort: strategy === "off" ? "" : current.reasoningEffort,
    }))
  }

  const editActions = <>
      {dirty ? (
        <div className="flex items-center justify-center gap-1">
          <Button
            type="button"
            size="icon-sm"
            variant="outline"
            disabled={saving || !draftValid}
            onClick={() => onSave(keyData.id, draft)}
            aria-label={invalidStrategy ? `${keyData.name} 将异常模型映射修复为按下游请求选择` : `${keyData.name} 保存模型映射`}
            title={invalidStrategy ? "修复为按下游请求选择" : draftValid ? "保存模型映射" : "请补全模型映射选项"}
          >
            <Save className="size-3.5" />
          </Button>
          {!invalidStrategy ? (
            <Button
              type="button"
              size="icon-sm"
              variant="outline"
              disabled={saving}
              onClick={() => setDraft(persisted.draft)}
              aria-label={`${keyData.name} 取消模型映射修改`}
              title="取消模型映射修改"
            >
              <X className="size-3.5" />
            </Button>
          ) : null}
        </div>
      ) : null}
  </>

  return (
    <div className="flex min-h-12 w-full min-w-0 flex-col justify-center gap-1">
      <div className="flex h-6 w-full items-center justify-between gap-2">
        <span className={cn("min-w-0 truncate text-sm", invalidStrategy ? "font-medium text-destructive" : enabled ? "font-medium text-foreground" : "text-muted-foreground")} title={invalidStrategy ? undefined : enabled ? `当前策略：${modelMappingLabel(draft.strategy)}` : "按下游请求选择模型"}>
          {invalidStrategy ? "模型映射异常" : enabled ? modelMappingLabel(draft.strategy) : "跟随请求"}
        </span>
        <Switch
          checked={invalidStrategy !== null || enabled}
          disabled={saving || (invalidStrategy === null && !mappingSupported && !enabled)}
          onCheckedChange={(checked) => {
            if (invalidStrategy !== null) {
              if (!checked) onSave(keyData.id, setModelMappingEnabled(draft, false, keyData.app))
              return
            }
            setDraft((current) => setModelMappingEnabled(current, checked, keyData.app))
            if (checked) setMappingEditorOpen(true)
          }}
          aria-label={invalidStrategy ? `${keyData.name} 关闭并修复异常模型映射` : `${keyData.name} ${enabled ? "关闭" : "启用"}模型映射`}
          aria-invalid={invalidStrategy ? true : undefined}
          aria-describedby={describedBy}
        />
      </div>
      {invalidStrategy ? (
        <div id={invalidReasonID} className="flex w-full flex-col items-center gap-1 text-center text-[10px] text-destructive">
          <Badge variant="outline" className="h-6 max-w-full border-destructive/40 px-1.5 text-[10px] text-destructive">{"历史映射策略异常"}</Badge>
          <span className="max-w-full truncate" title={invalidStrategy}>{`未知策略：${invalidStrategy}`}</span>
          <span>{"关闭开关即可修复"}</span>
        </div>
      ) : null}
      {mappingUnsupportedReason ? (
        <Badge id={unsupportedReasonID} variant="outline" className="min-h-6 max-w-full whitespace-normal px-1.5 py-1 text-center text-[10px] leading-4 text-muted-foreground">
          {mappingUnsupportedReason}
        </Badge>
      ) : null}
      {manualModelUnsupported && mappingSupported ? (
        <Badge id={manualModelReasonID} variant="outline" className="min-h-6 max-w-full whitespace-normal border-destructive/40 px-1.5 py-1 text-center text-[10px] leading-4 text-destructive">
          {draft.model.trim() ? "固定模型不在当前候选组" : "请选择当前候选组支持的模型"}
        </Badge>
      ) : null}
      {enabled ? <Popover open={mappingEditorOpen} onOpenChange={setMappingEditorOpen}>
        <PopoverTrigger asChild><Button type="button" size="sm" variant="ghost" className="h-6 w-full min-w-0 justify-between px-0 text-xs text-muted-foreground" aria-label={`${keyData.name} 编辑模型映射`} title={previewDetail}><span className="truncate">{dirty ? "待保存" : mappingError ? "映射未就绪" : `${previewModel}${previewReasoning ? ` · ${mappingReasoningLabel(keyData.app, previewReasoning)}` : ""}`}</span><Pencil className="size-3.5" /></Button></PopoverTrigger>
        <PopoverContent className="w-80 space-y-3" align="start">
          <div className="text-sm font-semibold">{keyData.name} · 模型映射</div>
      {enabled ? (
        <div className="flex w-full flex-col gap-1.5">
          <Select value={draft.strategy} onValueChange={changeStrategy} disabled={saving || !mappingSupported}>
            <SelectTrigger className="h-8 w-full text-xs" aria-label={`${keyData.name} 模型映射策略`} aria-describedby={mappingUnsupportedReason ? unsupportedReasonID : undefined}><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="off">{"按下游请求选择"}</SelectItem>
              <SelectItem value="iq" disabled={!automaticMappingSupported}>{"智商优先"}</SelectItem>
              <SelectItem value="cost" disabled={!automaticMappingSupported}>{"费用优先"}</SelectItem>
              <SelectItem value="time" disabled={!automaticMappingSupported}>{"耗时优先"}</SelectItem>
              <SelectItem value="balanced" disabled={!automaticMappingSupported}>{"综合平衡"}</SelectItem>
              <SelectItem value="manual" disabled={!mappingSupported}>{"手动固定"}</SelectItem>
            </SelectContent>
          </Select>
          {draft.strategy === "manual" ? (
            <Select
              value={draft.model || "__select_model__"}
              onValueChange={(value) => setDraft((current) => ({ ...current, model: value === "__select_model__" ? "" : value }))}
              disabled={saving || !mappingSupported || mappingModels.length === 0}
            >
              <SelectTrigger className="h-8 min-w-0 text-xs" aria-label={`${keyData.name} 固定模型`} aria-describedby={describedBy} title={draft.model || "选择固定模型"}><SelectValue placeholder="选择模型" /></SelectTrigger>
              <SelectContent className="max-h-72">
                <SelectItem value="__select_model__" disabled>{mappingModels.length === 0 ? "先同步模型" : "选择模型"}</SelectItem>
                {mappingModels.map((model) => <SelectItem key={model} value={model} title={model}>{model}</SelectItem>)}
              </SelectContent>
            </Select>
          ) : null}
          {keyData.app === "claude" && draft.strategy !== "manual" ? (
            <Badge variant="outline" className="h-8 justify-center text-[11px] text-destructive" title="Claude Radar 未提供真实 API 模型 ID，自动映射暂不可执行">{"API 模型未绑定"}</Badge>
          ) : (
            <Select
              value={draft.reasoningEffort || (draft.strategy === "manual" ? "__select_reasoning__" : "__radar_auto__")}
              onValueChange={(value) => setDraft((current) => ({
                ...current,
                reasoningEffort: value === "__radar_auto__" || value === "__select_reasoning__" ? "" : value,
              }))}
              disabled={saving || !mappingSupported}
            >
              <SelectTrigger className="h-8 min-w-0 text-xs" aria-label={`${keyData.name} 映射思考`} aria-describedby={mappingUnsupportedReason ? unsupportedReasonID : undefined} title={draft.reasoningEffort ? mappingReasoningLabel(keyData.app, draft.reasoningEffort) : draft.strategy === "manual" ? "选择思考强度" : "Radar 自动"}><SelectValue /></SelectTrigger>
              <SelectContent>{reasoningOptions.map((option) => <SelectItem key={option.value || (draft.strategy === "manual" ? "__select_reasoning__" : "__radar_auto__")} value={option.value || (draft.strategy === "manual" ? "__select_reasoning__" : "__radar_auto__")} disabled={draft.strategy === "manual" && option.value === ""}>{option.label}</SelectItem>)}</SelectContent>
            </Select>
          )}
          {!dirty && mappingSupported ? (
            <span className={cn("max-w-full truncate text-center text-[11px]", mappingError ? "text-destructive" : "text-muted-foreground")} title={previewDetail || undefined}>
              {mappingError ? `原因 · ${modelMappingErrorSummary(mappingError)}` : `当前 · ${previewModel} · ${mappingReasoningLabel(keyData.app, previewReasoning)}`}
            </span>
          ) : null}
        </div>
      ) : null}
          {editActions}
        </PopoverContent>
      </Popover> : null}
      {!enabled && editActions}
    </div>
  )
}

function ManualRoutePickerDialog({
  keyData,
  candidates,
  selected,
  mappedTargetPreview,
  loading,
  error,
  saving,
  onClose,
  onSelect,
}: {
  keyData: GatewayDownstreamKey | null
  candidates: GatewayRouteCandidate[]
  selected: GatewayRouteCandidate | null
  mappedTargetPreview: GatewayManualMappedTargetPreview | null
  loading: boolean
  error: string | null
  saving: boolean
  onClose: () => void
  onSelect: (key: GatewayDownstreamKey, candidate: GatewayRouteCandidate) => void
}) {
  const [search, setSearch] = useState("")
  const keyGroupName = keyData?.key_group_name?.trim() || "未分组"
  const title = keyData ? `${keyGroupName} / ${keyData.name} · 固定候选` : "固定候选"
  const normalizedSearch = search.trim().toLowerCase()
  const filteredCandidates = useMemo(() => normalizedSearch ? candidates.filter((candidate) => (
    `${routeCandidateChannelName(candidate)} ${routeCandidateGroupName(candidate)} ${candidate.gateway_app ?? ""}`.toLowerCase().includes(normalizedSearch)
  )) : candidates, [candidates, normalizedSearch])
  useEffect(() => setSearch(""), [keyData?.id])
  return (
    <Dialog open={keyData != null} onOpenChange={(open) => { if (!open && !saving) onClose() }}>
      <DialogContent className="w-[min(960px,calc(100vw-2rem))] max-w-none gap-0 overflow-hidden p-0 sm:max-w-none">
        <DialogHeader className="border-b border-border bg-muted/20 px-5 py-4">
          <DialogTitle className="truncate" title={title}>{title}</DialogTitle>
        </DialogHeader>
        <div className="border-b border-border px-4 py-3">
          <div className="relative">
            <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input value={search} onChange={(event) => setSearch(event.target.value)} className="h-9 pl-9" placeholder="搜索渠道或分组" aria-label="搜索固定候选" />
          </div>
        </div>
        <div className="max-h-[min(32rem,calc(100vh-12rem))] space-y-2 overflow-auto p-4">
          {loading ? <div className="py-12 text-center text-sm text-muted-foreground">{"加载候选中"}</div> : null}
          {!loading && error ? <div className="py-12 text-center text-sm text-destructive">{error}</div> : null}
          {!loading && !error && mappedTargetPreview?.mapping_state === "unresolved" ? (
            <div className="rounded-md border border-warning/25 bg-muted/25 px-3 py-2 text-xs text-muted-foreground">
              {"模型映射目标暂未解析；当前不会据此阻断选择，保存时服务端仍会重新校验。"}
            </div>
          ) : null}
          {!loading && !error && filteredCandidates.map((candidate) => {
            const mappedTargetState = manualMappedTargetSelectionState(candidate, mappedTargetPreview)
            const selectable = mappedTargetState.selectable
            const smartRouteUnavailable = !clientImportCandidateRoutable(candidate)
            const isSelected = selected != null && routeCandidateKey(selected) === routeCandidateKey(candidate)
            const blockingReason = selectable ? "" : mappedTargetState.blockingReasons.map(candidateAvailabilityLabel).join("、")
            const rateSnapshotMissing = candidateAvailabilityReasons(candidate).includes("no_rate_snapshot")
            const candidateTitle = `${routeCandidateChannelName(candidate)} / ${routeCandidateGroupName(candidate)}`
            return (
              <button
                key={routeCandidateKey(candidate)}
                type="button"
                disabled={!selectable || saving || keyData == null}
                onClick={() => { if (keyData) onSelect(keyData, candidate) }}
                className={cn(
                  "grid min-h-12 w-full min-w-[760px] grid-cols-[minmax(8rem,1.1fr)_minmax(10rem,1.4fr)_max-content_max-content_max-content] items-center gap-4 rounded-md border px-4 py-2 text-left transition-colors",
                  isSelected ? "border-brand/45 bg-brand/5" : "border-border bg-background hover:bg-muted/35",
                  !selectable && "cursor-not-allowed border-warning/25 bg-muted/25",
                )}
                title={blockingReason ? `${candidateTitle} · 不可选择：${blockingReason}` : candidateTitle}
                aria-label={blockingReason ? `${candidateTitle}。当前不可选择。阻断原因：${blockingReason}` : undefined}
              >
                <span className="min-w-0 truncate text-sm font-bold">{routeCandidateChannelName(candidate) || `渠道 ${candidate.channel_id}`}</span>
                <span className="min-w-0 truncate text-xs font-normal text-foreground">{routeCandidateGroupName(candidate) || "默认分组"}</span>
                <span className="whitespace-nowrap text-xs text-muted-foreground" title={candidate.effective_price_basis === "model_price_usd_per_million" ? `缓存与资金系数已计入，输入占比 ${Math.round((candidate.price_input_share ?? 0.5) * 100)}%；用于比较，不是本次账单` : "按分组倍率比较；缺少完整可比模型价格时保守回退"}>{rateSnapshotMissing ? "倍率 暂无" : candidate.effective_price_basis === "model_price_usd_per_million" ? `参考 $${manualRoutePrice(candidate)}/M` : `倍率 ${manualRoutePrice(candidate)}x`}</span>
                <span className="whitespace-nowrap text-xs text-muted-foreground">{`延迟 ${manualRouteLatency(candidate.avg_latency_ms)}`}</span>
                <span className="flex shrink-0 items-center justify-end gap-1.5 whitespace-nowrap">
                  {isSelected ? <Badge variant="secondary">{"当前固定"}</Badge> : null}
                  {!selectable ? <Badge variant="outline">{"当前不可选择"}</Badge> : null}
                  {selectable && candidate.disabled_for_key ? <Badge variant="outline">{"Key 禁用 · 固定忽略"}</Badge> : null}
                  {selectable && candidate.cooling_down ? <Badge variant="outline">{"故障隔离 · 固定忽略"}</Badge> : null}
                  {selectable && rateSnapshotMissing ? <Badge variant="outline">{"倍率快照缺失 · 固定直通"}</Badge> : null}
                  {selectable && smartRouteUnavailable && !rateSnapshotMissing ? <Badge variant="outline">{"状态异常 · 仍可固定"}</Badge> : null}
                  {selectable && !candidate.disabled_for_key && !candidate.cooling_down && !rateSnapshotMissing && !smartRouteUnavailable ? <Badge variant="outline">{"可选择"}</Badge> : null}
                </span>
                {blockingReason ? <span className="col-span-5 text-xs leading-5 text-warning">{`阻断原因：${blockingReason}`}</span> : null}
                {selectable && rateSnapshotMissing ? <span className="col-span-5 text-xs leading-5 text-muted-foreground">{"提示：暂无倍率快照不会阻止固定选择；固定请求仍只向此目标发起一次转发。"}</span> : null}
              </button>
            )
          })}
          {!loading && !error && candidates.length === 0 ? <div className="py-12 text-center text-sm text-muted-foreground">{"当前 Key 没有可固定的候选"}</div> : null}
          {!loading && !error && candidates.length > 0 && filteredCandidates.length === 0 ? <div className="py-12 text-center text-sm text-muted-foreground">{"没有匹配的固定候选"}</div> : null}
        </div>
        <div className="flex justify-end border-t border-border px-5 py-3">
          <Button type="button" variant="outline" onClick={onClose} disabled={saving}>{"关闭"}</Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}

type CandidateManagementTab = "available" | "cooling" | "disabled" | "unavailable"

const candidateManagementTabs: Array<{ id: CandidateManagementTab; label: string }> = [
  { id: "available", label: "可用候选" },
  { id: "cooling", label: "故障冷却" },
  { id: "disabled", label: "禁用候选" },
  { id: "unavailable", label: "不可用候选" },
]

const candidateManagementFilterAll = candidateGroupMembershipFilterAll

function CandidateManagementDialog({
  keyData,
  candidateGroups,
  candidateGroupsLoading,
  candidateInventoryLoading,
  groupCandidates,
  renderRouteCandidateItem,
  onClose,
}: {
  keyData: GatewayDownstreamKey | null
  candidateGroups: GatewayCandidateGroup[]
  candidateGroupsLoading: boolean
  candidateInventoryLoading: boolean
  groupCandidates: GatewayRouteCandidate[]
  renderRouteCandidateItem: (
    key: GatewayDownstreamKey,
    candidate: GatewayRouteCandidate,
    rank: number,
    onChanged?: (disabled: boolean) => void,
    mutationsDisabled?: boolean,
  ) => ReactNode
  onClose: () => void
}) {
  const [selectedModel, setSelectedModel] = useState("")
  const [modelContext, setModelContext] = useState<GatewayCandidatePreviewModelContext>("requested")
  const [activeTab, setActiveTab] = useState<CandidateManagementTab>("available")
  const [modelPickerOpen, setModelPickerOpen] = useState(false)
  const [modelPinned, setModelPinned] = useState(false)
  const [modelSearch, setModelSearch] = useState("")
  const initializedScopeRef = useRef("")
  const [membershipSearch, setMembershipSearch] = useState("")
  const [membershipProtocol, setMembershipProtocol] = useState(candidateManagementFilterAll)
  const [disabledOverrides, setDisabledOverrides] = useState<Map<string, boolean>>(() => new Map())
  const initializedKeyIDRef = useRef<number | null>(null)
  const initializedPreferenceContextRef = useRef<GatewayCandidatePreviewModelContext | null>(null)
  const inventoryLoading = candidateGroupsLoading || candidateInventoryLoading
  const configuredModelOptions = useMemo(
    () => keyData ? candidateManagementModelOptions(keyData, candidateGroups, groupCandidates) : [],
    [candidateGroups, groupCandidates, keyData],
  )
  const fallbackContext = candidatePreviewFallbackContext(keyData)
  const preferenceScope = `${keyData?.id}:${keyData?.candidate_group_id}:${keyData?.app}:${keyData?.model_family}:${fallbackContext.context}`
  useEffect(() => {
    const keyID = keyData?.id ?? null
    const keyChanged = initializedKeyIDRef.current !== keyID
    const preferenceContextChanged = initializedPreferenceContextRef.current !== fallbackContext.context || initializedScopeRef.current !== preferenceScope
    if (keyID == null) {
      initializedKeyIDRef.current = null
      initializedPreferenceContextRef.current = null
      initializedScopeRef.current = ""
      setModelPinned(false)
      setSelectedModel("")
      setModelContext("requested")
      setModelPickerOpen(false)
      return
    }
    if (keyChanged) {
      setActiveTab("available")
      setModelPickerOpen(false)
    }
    if (inventoryLoading) return
    // Keep a manual preview choice through background mapping changes and
    // incomplete inventory snapshots. Missing models stop the preview request.
    if (!keyChanged && !preferenceContextChanged && modelPinned) return
    initializedKeyIDRef.current = keyID
    initializedPreferenceContextRef.current = fallbackContext.context
    initializedScopeRef.current = preferenceScope
    if (keyChanged || preferenceContextChanged) setModelPinned(false)
    if (keyChanged) {
      const saved = readCandidatePreviewModelPreference(keyID, fallbackContext.context)
      if (saved) {
        const current = configuredModelOptions.find(model => model.toLowerCase() === saved.toLowerCase())
        setSelectedModel(current || saved)
        setModelContext(fallbackContext.context)
        setModelPinned(true)
        return
      }
    }
    // Probe configuration loads independently from downstream keys. An empty
    // model set cannot prove that a saved value is stale, so wait for a
    // synchronized candidate model before validating or deleting it.
    if (configuredModelOptions.length === 0) {
      if (inventoryLoading) return
      setSelectedModel("")
      setModelContext(fallbackContext.context)
      return
    }
    if (!keyChanged && !preferenceContextChanged && configuredModelOptions.some((model) => model.toLowerCase() === selectedModel.trim().toLowerCase())) return
    const resolved = resolveCandidatePreviewModelPreference({
      downstreamKeyID: keyID,
      validModels: configuredModelOptions,
      preferenceContext: fallbackContext.context,
      mappedModel: fallbackContext.mappedModel,
      recentRequestModel: fallbackContext.recentRequestModel,
      defaultContext: fallbackContext.context,
    })
    setSelectedModel(resolved.model)
    setModelContext(resolved.context)
    setModelPinned(resolved.source === "saved")
  }, [configuredModelOptions, fallbackContext.context, fallbackContext.mappedModel, fallbackContext.recentRequestModel, inventoryLoading, keyData?.id, selectedModel, modelPinned, preferenceScope])

  useEffect(() => {
    setMembershipSearch("")
    setMembershipProtocol(candidateManagementFilterAll)
    setDisabledOverrides(new Map())
  }, [keyData?.id])

  const previewModelAvailable = configuredModelOptions.some(model => model.toLowerCase() === selectedModel.trim().toLowerCase())
  const preview = useGatewayCandidateGroupPreview(keyData?.id ?? null, previewModelAvailable ? selectedModel : "", modelContext, keyData != null && previewModelAvailable)
  const previewIdentity = `${keyData?.id ?? 0}:${preview.queryPath ?? ""}`
  const activePreviewIdentityRef = useRef(previewIdentity)
  activePreviewIdentityRef.current = previewIdentity
  const previewResolved = preview.queryPath != null && preview.resolvedPath === preview.queryPath
  const previewCandidates = useMemo(() => previewModelAvailable && selectedModel && previewResolved
    ? safeRouteCandidates(preview.data)
    : [], [preview.data, previewResolved, selectedModel, previewModelAvailable])
  const expectedMemberCount = keyData ? candidateGroupMemberCount(keyData, candidateGroups) : 0
  const title = keyData ? `${keyGroupDisplayName(keyData, [])} / ${keyData.name} · 候选管理` : "候选管理"
  const modelOptions = configuredModelOptions
  const modelPreviewLoading = Boolean(keyData && selectedModel && previewModelAvailable && !preview.error && (preview.loading || !previewResolved))
  const inventoryCandidates = useMemo(() => keyData && !inventoryLoading
    ? candidateManagementInventoryCandidates(keyData, candidateGroups, groupCandidates, previewCandidates, disabledOverrides)
    : [], [candidateGroups, disabledOverrides, groupCandidates, inventoryLoading, keyData, previewCandidates])
  const inventoryRows = useMemo(() => buildCandidateGroupMembershipRows(inventoryCandidates), [inventoryCandidates])
  const protocols = useMemo(() => candidateGroupMembershipProtocolOptions(inventoryRows), [inventoryRows])
  const filteredRows = useMemo(() => filterCandidateGroupMembershipRows(inventoryRows, {
    search: membershipSearch,
    status: candidateManagementFilterAll,
    protocol: membershipProtocol,
    channelID: candidateManagementFilterAll,
  }), [inventoryRows, membershipProtocol, membershipSearch])
  const categorized = useMemo(() => candidateManagementCategorizeRows(filteredRows, membershipProtocol), [filteredRows, membershipProtocol])
  const activeRows = useMemo(() => filteredRows.filter((row) => candidateManagementRowCategory(row, membershipProtocol) === activeTab), [activeTab, filteredRows, membershipProtocol])
  const rankedRows = useMemo(() => candidateManagementRankedRows(
    inventoryRows,
    previewCandidates,
    groupCandidates,
    membershipProtocol === candidateManagementFilterAll ? keyData?.app : (membershipProtocol as GatewayApp),
  ), [groupCandidates, inventoryRows, keyData?.app, membershipProtocol, previewCandidates])
  const activeRowKeys = useMemo(() => new Set(activeRows.map((row) => row.key)), [activeRows])
  const activeRankedRows = useMemo(() => rankedRows.filter(({ row }) => activeRowKeys.has(row.key)), [activeRowKeys, rankedRows])

  useEffect(() => {
    if (membershipProtocol === candidateManagementFilterAll || protocols.includes(membershipProtocol)) return
    setMembershipProtocol(candidateManagementFilterAll)
  }, [membershipProtocol, protocols])

  function updateDisabledCandidate(candidate: GatewayRouteCandidate, disabled: boolean) {
    if (inventoryLoading) return
    const memberKey = candidateManagementMemberKey(candidate.channel_id, candidate.group_name)
    setDisabledOverrides((current) => {
      const next = new Map(current)
      next.set(memberKey, disabled)
      return next
    })
    if (activePreviewIdentityRef.current !== previewIdentity) return
    preview.setData((current) => safeRouteCandidates(current).map((item) => (
      routeCandidateKey(item) === routeCandidateKey(candidate)
        ? { ...item, disabled_for_key: disabled }
        : item
    )))
    preview.refetch()
  }

  return (
    <>
      <Dialog open={keyData != null} onOpenChange={(open) => { if (!open) onClose() }}>
        <DialogContent className="flex h-[min(44rem,calc(100vh-2rem))] w-[min(1280px,90vw)] max-w-none flex-col gap-0 overflow-hidden p-0 sm:max-w-none">
          <DialogHeader className="gap-3 border-b border-border bg-brand/5 px-5 py-4">
            <div className="flex min-w-0 flex-wrap items-center justify-between gap-3">
              <DialogTitle className="min-w-0 truncate" title={title}>{title}</DialogTitle>
              <div className="flex shrink-0 items-center gap-2">
                <Badge variant="secondary">{"禁用仅影响当前 Key"}</Badge>
                <Badge variant="outline">{inventoryLoading ? "候选库存加载中" : `共 ${expectedMemberCount} 个成员`}</Badge>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">路由预览模型</span>
              <Popover open={modelPickerOpen} onOpenChange={open => { setModelPickerOpen(open); if (open) setModelSearch("") }}>
                <PopoverTrigger asChild><Button variant="outline" className="h-9 max-w-lg justify-between gap-3 font-mono text-sm" aria-label="选择路由预览模型"><span className="truncate">{selectedModel || "选择模型"}</span><ChevronDown className="size-4" /></Button></PopoverTrigger>
                <PopoverContent className="w-96 p-2" align="start">
                  <Input value={modelSearch} onChange={event => setModelSearch(event.target.value)} placeholder="搜索模型" aria-label="搜索路由预览模型" />
                  <div className="mt-2 max-h-64 overflow-y-auto">
                    {modelOptions.filter(model => model.toLowerCase().includes(modelSearch.toLowerCase())).map(model => <Button key={model} variant={model === selectedModel ? "secondary" : "ghost"} className="h-9 w-full justify-start font-mono text-sm" onClick={() => {
                      setModelPinned(true)
                      setSelectedModel(model)
                      if (keyData) writeCandidatePreviewModelPreference(keyData.id, modelContext, model)
                      setModelPickerOpen(false)
                    }}><span className="truncate">{model}</span></Button>)}
                    {modelOptions.length === 0 && <p className="p-3 text-sm text-muted-foreground">暂无已同步模型</p>}
                  </div>
                </PopoverContent>
              </Popover>
              {modelPinned && <Badge variant="outline">已固定预览</Badge>}
              <HelpHint label="路由预览模型">只模拟当前 Key 的候选排序，不修改实际路由；手选模型会按 Key 记住。</HelpHint>
            </div>
          </DialogHeader>
          <div className="flex items-center gap-2 border-b border-border px-5 py-2">
            {candidateManagementTabs.map((tab) => {
              const active = activeTab === tab.id
              return (
                <Button
                  key={tab.id}
                  type="button"
                  variant={active ? "default" : "outline"}
                  className="h-8 min-w-0 justify-between gap-2 px-3 text-sm"
                  onClick={() => setActiveTab(tab.id)}
                >
                  <span className="truncate">{tab.label}</span>
                  <Badge variant={active ? "secondary" : "outline"} className="h-5 shrink-0 px-1.5 text-[10px]">{categorized[tab.id].length}</Badge>
                </Button>
              )
            })}
          </div>
          <div className="border-b border-border bg-muted/15 px-5 py-3">
            <div className="grid gap-2 md:grid-cols-[minmax(14rem,1fr)_11rem_auto]">
              <label className="relative block">
                <span className="sr-only">{"搜索候选"}</span>
                <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" aria-hidden="true" />
                <Input value={membershipSearch} onChange={(event) => setMembershipSearch(event.target.value)} placeholder="搜索渠道、上游分组或协议" aria-label="搜索候选渠道、分组或协议" className="h-9 bg-background pl-8 text-xs" />
              </label>
              <Select value={membershipProtocol} onValueChange={setMembershipProtocol}>
                <SelectTrigger aria-label="筛选候选协议" className="h-9 bg-background text-xs"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value={candidateManagementFilterAll}>{"全部协议"}</SelectItem>
                  {protocols.map((protocol) => <SelectItem key={protocol} value={protocol}>{protocol}</SelectItem>)}
                </SelectContent>
              </Select>
              <div className="flex items-center justify-end text-xs text-muted-foreground tabular-nums">
                <span>{`当前 ${activeRows.length}/${expectedMemberCount}`}</span>
              </div>
            </div>

          </div>
          <div className="min-h-0 flex-1 overflow-auto">
            {modelPinned && selectedModel && !previewModelAvailable && !inventoryLoading ? <div className="border-b px-5 py-2 text-sm text-warning">固定预览模型当前未同步，暂显示静态候选；同步恢复后继续使用 {selectedModel}。</div> : null}
            {modelPreviewLoading ? <div className="border-b border-brand/15 bg-brand/5 px-5 py-2 text-xs text-muted-foreground">{"正在按预览模型更新可用性；静态候选清单仍可管理。"}</div> : null}
            {selectedModel && preview.error ? <div className="flex items-center justify-between gap-3 border-b border-warning/25 bg-warning/5 px-5 py-2 text-xs text-muted-foreground"><span>{"模型预览暂不可用，当前显示静态候选清单。"}</span><Button type="button" size="sm" variant="outline" className="h-7 px-2 text-xs" onClick={preview.refetch}><RefreshCw className="size-3.5" />{"重试"}</Button></div> : null}
            {!selectedModel && modelOptions.length === 0 ? <div className="border-b border-border px-5 py-2 text-xs text-muted-foreground">{"当前没有已同步模型；仍可在静态清单中查看和管理当前 Key 的候选禁用。"}</div> : null}
            {inventoryLoading ? <div className="flex min-h-48 items-center justify-center gap-2 px-5 text-center text-sm text-muted-foreground"><RefreshCw className="size-4 animate-spin" aria-hidden="true" />{"候选库存加载中，暂不能修改候选状态。"}</div> : (
              <div>
                <div className="sticky top-0 z-10 grid min-w-[1080px] grid-cols-[2rem_minmax(11rem,1.5fr)_6rem_6rem_5rem_4rem_minmax(16rem,1fr)] gap-3 border-b border-border bg-card px-3 py-2 text-sm text-muted-foreground"><span>#</span><span>渠道 / 分组</span><span>状态</span><span>价格</span><span>延迟</span><span>得分</span><span className="text-right">操作</span></div>
                {activeRankedRows.map(({ row, candidate, rank }) => {
                  if (!keyData) return null
                  const reason = candidateManagementRowReason(row, membershipProtocol)
                  return (
                    <article key={row.key} title={reason || undefined}>
                      {candidateManagementInventoryCandidateIsCurrent(candidate) ? renderRouteCandidateItem(
                        keyData,
                        candidate,
                        rank,
                        (disabled) => updateDisabledCandidate(candidate, disabled),
                        inventoryLoading,
                      ) : <div className="grid items-start gap-2 rounded-md border border-dashed border-border bg-muted/20 px-2 py-2 text-xs text-muted-foreground sm:grid-cols-[auto_minmax(0,1fr)]">
                        <Badge variant="outline" className="h-5 min-w-8 justify-center px-1 text-[10px]">{"-"}</Badge>
                        <span>{`${row.channelName} / ${row.groupName} · 当前不可见，无法显示渠道类型、倍率或执行操作。`}</span>
                      </div>}

                    </article>
                  )
                })}
              </div>
            )}
            {!inventoryLoading && activeRankedRows.length === 0 ? <div className="flex min-h-48 items-center justify-center px-5 text-center text-sm text-muted-foreground">{inventoryRows.length === 0 ? "当前 Key 没有候选成员" : `当前没有${candidateManagementTabLabel(activeTab)}`}</div> : null}
          </div>
          <div className="flex justify-end border-t border-border px-5 py-3">
            <Button type="button" variant="outline" onClick={onClose}>{"关闭"}</Button>
          </div>
        </DialogContent>
      </Dialog>

    </>
  )
}

function KeyStat({ label, value, hint, active, onClick, warning = false }: {
  label: string
  value: string
  hint: string
  active: boolean
  onClick: () => void
  warning?: boolean
}) {
  return <button type="button" aria-pressed={active} onClick={onClick} title={hint} className={cn("flex h-9 items-center gap-2 rounded-lg px-3 text-sm transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring", active ? "bg-accent font-medium text-accent-foreground" : "text-muted-foreground hover:bg-muted hover:text-foreground")}><span>{label}</span><span className={cn("text-xs tabular-nums", warning && !active && "text-warning")}>{value}</span></button>
}

function safeRouteCandidates(candidates: GatewayRouteCandidate[] | null | undefined) {
  return Array.isArray(candidates) ? candidates : []
}

function routeCandidateGroupName(candidate: GatewayRouteCandidate) {
  return String(candidate.group_name ?? "").trim()
}

function routeCandidateChannelName(candidate: GatewayRouteCandidate) {
  return String(candidate.channel_name ?? "").trim()
}

function routeCandidateKey(candidate: GatewayRouteCandidate) {
  return `${candidate.channel_id}::${routeCandidateGroupName(candidate)}`
}

const pendingRoutePreview: RoutePreviewState = {
  data: null,
  loading: true,
  error: null,
  mappedTargetData: null,
  mappedTargetLoading: true,
  mappedTargetError: null,
}

function selectedManualCandidate(key: GatewayDownstreamKey, candidates: GatewayRouteCandidate[]) {
  if (!key.manual_channel_id || !key.manual_group_name) return null
  return candidates.find((candidate) => candidate.channel_id === key.manual_channel_id && routeCandidateGroupName(candidate) === key.manual_group_name) ?? null
}

function candidatesForKey(key: GatewayDownstreamKey, candidates: GatewayRouteCandidate[]) {
  const sameAppCandidates = candidates.filter((candidate) => !candidate.gateway_app || candidate.gateway_app === key.app)
  const family = keyModelFamily(key)
  const sameFamilyCandidates = sameAppCandidates.filter((candidate) => {
    const families = Array.isArray(candidate.model_families) ? candidate.model_families.map((item) => String(item ?? "").toLowerCase().trim()).filter(Boolean) : []
    return families.length === 1 && families[0] === family
  })
  const scopedCandidates = family ? sameFamilyCandidates : sameAppCandidates.length > 0 ? sameAppCandidates : candidates
  return scopedCandidates.map((candidate) => ({
    ...candidate,
    disabled_for_key: candidate.disabled_for_key || routeExcludedForKey(key, candidate),
  }))
}

function manualCandidatesForKey(key: GatewayDownstreamKey, candidates: GatewayRouteCandidate[]) {
  return candidates.map((candidate) => ({
    ...candidate,
    disabled_for_key: candidate.disabled_for_key || routeExcludedForKey(key, candidate),
  }))
}

function routeCandidatesForKey(key: GatewayDownstreamKey, candidates: GatewayRouteCandidate[]) {
  return key.smart_routing ? candidatesForKey(key, candidates) : manualCandidatesForKey(key, candidates)
}

function keyModelFamily(key: GatewayDownstreamKey) {
  const family = String(key.model_family ?? "").trim().toLowerCase()
  if (family) return family
  if (key.app === "claude" || key.app === "gemini") return key.app
  return "codex"
}

function candidateGroupLabel(key: GatewayDownstreamKey, groups: GatewayCandidateGroup[]) {
  const resolved = key.candidate_group_name?.trim()
    || groups.find((group) => group.id === key.candidate_group_id)?.name
  if (key.candidate_target_source === "key_group_policy") {
    return resolved ? `分组策略 · ${resolved}` : "Key 分组策略 · 全部候选"
  }
  return resolved || "全部候选"
}

function candidateGroupMemberCount(key: GatewayDownstreamKey, groups: GatewayCandidateGroup[]) {
  const groupID = Number(key.candidate_group_id ?? 0)
  if (!Number.isFinite(groupID) || groupID <= 0) return 0
  return groups.find((group) => group.id === groupID)?.members?.length ?? 0
}

function candidatePreviewFallbackContext(key: GatewayDownstreamKey | null): {
  mappedModel: string
  recentRequestModel: string
  context: GatewayCandidatePreviewModelContext
} {
  if (!key) return { mappedModel: "", recentRequestModel: "", context: "requested" }
  const preview = key.model_mapping_preview
  const strategy = String(key.model_mapping_strategy ?? preview?.strategy ?? "off").trim().toLowerCase()
  const mappedModel = strategy === "manual"
    ? String(key.model_mapping_model ?? "").trim()
    : preview?.error ? "" : String(preview?.model ?? "").trim()
  return {
    mappedModel: strategy === "off" ? "" : mappedModel,
    recentRequestModel: String(key.candidate_preview_model ?? "").trim(),
    context: strategy !== "off" ? "mapped" : "requested",
  }
}

function candidateManagementModelOptions(key: GatewayDownstreamKey, groups: GatewayCandidateGroup[], candidates: GatewayRouteCandidate[]) {
  const groupID = Number(key.candidate_group_id ?? 0)
  const group = groups.find((item) => item.id === groupID)
  const memberKeys = new Set((group?.members ?? []).map((member) => `${member.channel_id}::${String(member.group_name ?? "").trim()}`))
  const models = new Set<string>()
  for (const candidate of candidatesForKey(key, candidates)) {
    if (!memberKeys.has(routeCandidateKey(candidate))) continue
    for (const model of Array.isArray(candidate.model_ids) ? candidate.model_ids : []) {
      const value = String(model ?? "").trim()
      if (value) models.add(value)
    }
  }
  return Array.from(models).sort((left, right) => left.localeCompare(right))
}

function candidateManagementCategorizeRows(
  rows: readonly CandidateGroupMembershipRow<CandidateManagementInventoryCandidate>[],
  protocol = candidateManagementFilterAll,
) {
  const categories: Record<CandidateManagementTab, CandidateGroupMembershipRow<CandidateManagementInventoryCandidate>[]> = {
    available: [],
    cooling: [],
    disabled: [],
    unavailable: [],
  }
  for (const row of rows) categories[candidateManagementRowCategory(row, protocol)].push(row)
  return categories
}

function candidateManagementRowCategory(
  row: CandidateGroupMembershipRow<CandidateManagementInventoryCandidate>,
  protocol = candidateManagementFilterAll,
): CandidateManagementTab {
  // A channel/group has one stable home even when the inventory contains more
  // than one protocol projection. Per-Key exclusion wins over cooling, then a
  // routable projection keeps it visible as available.
  const candidates = protocol === candidateManagementFilterAll
    ? row.candidates
    : row.candidates.filter((candidate) => candidate.gateway_app === protocol)
  if (candidates.some((candidate) => candidate.disabled_for_key)) return "disabled"
  if (candidates.some((candidate) => candidate.cooling_down)) return "cooling"
  if (candidates.some((candidate) => (
    candidateManagementInventoryCandidateIsCurrent(candidate)
    && clientImportCandidateRoutable(candidate)
  ))) return "available"
  return "unavailable"
}

function candidateManagementRepresentativeCandidate(
  row: CandidateGroupMembershipRow<CandidateManagementInventoryCandidate>,
  app: GatewayApp | undefined,
): CandidateManagementInventoryCandidate | null {
  return row.candidates.find((candidate) => candidate.gateway_app === app) ?? row.candidates[0] ?? null
}

type CandidateManagementRankedRow = {
  row: CandidateGroupMembershipRow<CandidateManagementInventoryCandidate>
  candidate: CandidateManagementInventoryCandidate
  rank: number
  inventoryOrder: number
  sourceOrder: number
}

function candidateManagementRankedRows(
  rows: readonly CandidateGroupMembershipRow<CandidateManagementInventoryCandidate>[],
  previewCandidates: readonly GatewayRouteCandidate[],
  originalInventoryCandidates: readonly GatewayRouteCandidate[],
  app: GatewayApp | undefined,
) {
  const rowKeys = new Set(rows.map((row) => row.key))
  const rankedMemberKeys = candidateManagementRankedMemberKeys(rowKeys, previewCandidates, originalInventoryCandidates)
  const rankByKey = new Map(rankedMemberKeys.map((key, index) => [key, index + 1]))

  const rankedRows: CandidateManagementRankedRow[] = []
  rows.forEach((row, sourceOrder) => {
    const candidate = candidateManagementRepresentativeCandidate(row, app)
    if (!candidate) return
    rankedRows.push({
      row,
      candidate,
      rank: candidateManagementInventoryCandidateIsCurrent(candidate)
        ? (rankByKey.get(row.key) ?? 0)
        : 0,
      inventoryOrder: rankByKey.get(row.key) ?? Number.MAX_SAFE_INTEGER,
      sourceOrder,
    })
  })

  return rankedRows.sort((left, right) => {
    if (left.rank > 0 && right.rank > 0) return left.rank - right.rank
    if (left.rank > 0) return -1
    if (right.rank > 0) return 1
    const leftCurrent = candidateManagementInventoryCandidateIsCurrent(left.candidate)
    const rightCurrent = candidateManagementInventoryCandidateIsCurrent(right.candidate)
    if (leftCurrent !== rightCurrent) return leftCurrent ? -1 : 1
    if (left.inventoryOrder !== right.inventoryOrder) return left.inventoryOrder - right.inventoryOrder
    return left.sourceOrder - right.sourceOrder
  })
}

function candidateManagementRowReason(
  row: CandidateGroupMembershipRow<CandidateManagementInventoryCandidate>,
  protocol = candidateManagementFilterAll,
) {
  const state = candidateGroupMembershipRowState(row, protocol)
  const labels = Array.from(new Set(state.availabilityReasons.map(candidateAvailabilityLabel)))
  if (state.snapshotMissing && !labels.includes(candidateAvailabilityLabel("no_rate_snapshot"))) {
    labels.unshift(candidateAvailabilityLabel("no_rate_snapshot"))
  }
  return labels.slice(0, 3).join("、")
}

function candidateManagementTabLabel(tab: CandidateManagementTab) {
  return candidateManagementTabs.find((item) => item.id === tab)?.label ?? "候选"
}

const keyGroupTonePalette = [
  { backgroundColor: "#eef6ff", borderColor: "#c5ddf5", color: "#315f88" },
  { backgroundColor: "#eef8f8", borderColor: "#c5e4e1", color: "#356b69" },
  { backgroundColor: "#f4f2fb", borderColor: "#ded8f0", color: "#605984" },
  { backgroundColor: "#fff7ed", borderColor: "#f3ddbd", color: "#815f3e" },
  { backgroundColor: "#f1f7f0", borderColor: "#d6e5d1", color: "#537150" },
] as const

function keyGroupTone(key: GatewayDownstreamKey) {
  const groupID = Number(key.key_group_id ?? 0)
  if (!Number.isFinite(groupID) || groupID <= 0) return keyGroupTonePalette[0]
  return keyGroupTonePalette[groupID % keyGroupTonePalette.length]
}

function candidateGroupMappingUnsupportedReason(
  key: GatewayDownstreamKey,
  groupID: string,
  groups: GatewayCandidateGroup[],
  loading: boolean,
) {
  if (loading) return "正在加载候选分组"
  if (groupID === "__all__") return "请先选择候选分组"
  const group = groups.find((item) => item.id === Number(groupID))
  if (!group) return "候选分组不存在"
  const family = String(group?.model_mapping_family ?? "").trim().toLowerCase()
  if (!family) return "候选分组未配置模型映射家族"
  if (family !== "codex" && family !== "claude") return "候选分组的模型映射家族不受支持"
  if (family !== keyModelFamily(key)) return "候选分组与当前模型家族不匹配"
  return null
}

function candidatesForCandidateGroup(candidates: GatewayRouteCandidate[], groupID: string, groups: GatewayCandidateGroup[]) {
  if (groupID === "__all__") return candidates
  const group = groups.find((item) => item.id === Number(groupID))
  if (!group) return []
  const memberKeys = new Set((group.members ?? []).map((member) => `${member.channel_id}::${String(member.group_name ?? "").trim()}`))
  return candidates.filter((candidate) => memberKeys.has(routeCandidateKey(candidate)))
}

function mappingCandidateModels(candidates: GatewayRouteCandidate[]) {
  return Array.from(new Set(candidates.flatMap((candidate) => Array.isArray(candidate.model_ids) ? candidate.model_ids : [])
    .map((model) => model.trim())
    .filter(Boolean)))
    .sort((left, right) => left.localeCompare(right))
}

function modelMappingReasoningOptions(app: GatewayApp, strategy: GatewayModelMappingStrategy, current: string) {
  const options = app === "claude"
    ? [
        { value: "1024", label: "1,024 Token" },
        { value: "2048", label: "2,048 Token" },
        { value: "4096", label: "4,096 Token" },
        { value: "8192", label: "8,192 Token" },
        { value: "16384", label: "16,384 Token" },
        { value: "32768", label: "32,768 Token" },
      ]
    : [
        { value: "minimal", label: "minimal" },
        { value: "low", label: "low" },
        { value: "medium", label: "medium" },
        { value: "high", label: "high" },
        { value: "xhigh", label: "xhigh" },
        { value: "max", label: "max" },
      ]
  options.unshift({ value: "", label: strategy === "manual" ? "选择思考强度" : "Radar 自动" })
  const saved = current.trim()
  if (saved && !options.some((option) => option.value === saved)) {
    options.push({ value: saved, label: `当前：${saved}` })
  }
  return options
}

function mappingReasoningLabel(app: GatewayApp, value: string) {
  const effort = value.trim()
  if (!effort) return "Radar 自动"
  if (app === "claude" && /^\d+$/.test(effort)) return `${compactTokenCount(Number(effort))} Token`
  return officialReasoningEffortLabel(effort)
}

function modelMappingErrorSummary(error: string) {
  const normalized = error.trim().toLowerCase()
  if (normalized.includes("radar is unavailable")) return "Radar 暂不可用"
  if (normalized.includes("radar has no model configurations") || normalized.includes("no usable model mapping option")) return "暂无符合策略的数据"
  if (normalized.includes("not bound to an api model")) return "API 模型未绑定"
  if (normalized.includes("requires a candidate group") || normalized.includes("no model mapping radar family")) return "候选分组未配置"
  if (normalized.includes("does not match downstream key")) return "模型族不匹配"
  if (normalized.includes("reasoning effort") || normalized.includes("thinking budget")) return "思考档位不支持"
  return "悬浮查看详情"
}

function manualRoutePrice(candidate: GatewayRouteCandidate) {
  const value = Number(candidate.effective_price ?? candidate.ratio ?? 0)
  if (!Number.isFinite(value)) return "0"
  return value.toLocaleString(undefined, { maximumFractionDigits: 4 })
}

function manualRouteLatency(value: number) {
  const ms = Number(value)
  if (!Number.isFinite(ms) || ms <= 0) return "暂无"
  return ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${Math.round(ms)}ms`
}

function modelMappingLabel(strategy: string | null | undefined) {
  const labels: Record<string, string> = {
    iq: "智商优先",
    cost: "费用优先",
    time: "耗时优先",
    balanced: "综合平衡",
    manual: "手动固定",
    off: "按请求模型",
  }
  return labels[String(strategy ?? "off").trim().toLowerCase()] ?? "按请求模型"
}

function ccSwitchProviderName(key: GatewayDownstreamKey, keyGroups: GatewayKeyGroup[]) {
  const groupName = keyGroupDisplayName(key, keyGroups)
  const keyName = key.name.trim() || "未命名"
  const family = modelFamilyLabel(key.model_family ?? key.app).replace(/\s*\/\s*/g, "-")
  return `BridgeOps-${groupName}-${keyName}-${family}`
}

function clientImportContextLabel(key: GatewayDownstreamKey, keyGroups: GatewayKeyGroup[]) {
  return `${keyGroupDisplayName(key, keyGroups)} / ${key.name.trim() || "未命名"}`
}

function keyGroupDisplayName(key: GatewayDownstreamKey, keyGroups: GatewayKeyGroup[]) {
  return key.key_group_name?.trim()
    || keyGroups.find((group) => group.id === key.key_group_id)?.name.trim()
    || "未分组"
}

function clientImportModelName(
  key: GatewayDownstreamKey,
  preferredCandidate?: GatewayRouteCandidate | null,
  candidates: GatewayRouteCandidate[] = [],
) {
  const mappingStrategy = String(key.model_mapping_preview?.strategy ?? key.model_mapping_strategy ?? "off").trim().toLowerCase()
  let mappedModel = ""
  if (mappingStrategy !== "off" && !key.model_mapping_preview?.error) {
    mappedModel = key.model_mapping_preview?.model?.trim() || key.model_mapping_model?.trim() || ""
  }
  return key.smart_routing
    ? clientImportDefaultModel(mappedModel, preferredCandidate, candidates, keyModelFamily(key))
    : clientImportFixedDefaultModel(mappedModel, preferredCandidate, candidates, keyModelFamily(key))
}

function enabledRouteCandidates(candidates: GatewayRouteCandidate[]) {
	return candidates.filter(clientImportCandidateRoutable)
}

function manualRouteCandidateRoutable(candidate: GatewayRouteCandidate) {
  return manualRouteCandidateSelectable(candidate)
}

function routeExcludedForKey(key: GatewayDownstreamKey, candidate: GatewayRouteCandidate) {
  const exclusions = Array.isArray(key.route_exclusions) ? key.route_exclusions : []
  if (exclusions.length === 0 || !candidate.channel_id) return false
  const groupName = routeCandidateGroupName(candidate)
  return exclusions.some((exclusion) => (
    exclusion.channel_id === candidate.channel_id
    && String(exclusion.group_name ?? "").trim() === groupName
  ))
}

function maskAPIKey(value: string | null | undefined) {
  const key = value ?? ""
  if (key.length <= 10) return "••••••"
  return `${key.slice(0, 8)}${"•".repeat(Math.min(24, Math.max(8, key.length - 8)))}`
}
