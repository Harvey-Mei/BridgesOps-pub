import { Fragment, useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from "react"
import {
  AlertTriangle,
  ChevronDown,
  ChevronRight,
  ChevronsDownUp,
  CircleCheck,
  History,
  ImageIcon,
  ListChecks,
  Pencil,
  Plus,
  RefreshCw,
  RotateCcw,
  Search,
  ShieldAlert,
  ShieldCheck,
  type LucideIcon,
  X,
} from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import type {
  GatewayImageCapabilityProtocol,
  GatewayImageOperationCapability,
  GatewayImageOperationCapabilityAudit,
  GatewayImageOperationTarget,
  GatewayImageTaskMode,
} from "@/lib/api-types"
import { useGatewayImageOperationCapabilities } from "@/lib/queries"
import {
  useGatewayImageOperationTargetInventory,
  type GatewayImageOperationTargetInventoryState,
} from "@/lib/image-operation-target-inventory"
import { dateTime } from "@/lib/format"
import { cn } from "@/lib/utils"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Checkbox } from "@/components/ui/checkbox"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"

export type CapabilityForm = {
  channelID: string
  expectedUpstreamGroupID: number
  expectedPresenceGeneration: number
  groupName: string
  modelName: string
  protocol: GatewayImageCapabilityProtocol
  taskMode: GatewayImageTaskMode
  idempotencyKeySupported: boolean
  idempotencyHeader: string
  taskIDReturned: boolean
  taskIDJSONPath: string
  taskQuerySupported: boolean
  taskQueryPathTemplate: string
  taskStatusJSONPath: string
  taskSuccessValues: string
  taskFailureValues: string
  expectedVersion: number
}

const emptyForm: CapabilityForm = {
  channelID: "",
  expectedUpstreamGroupID: 0,
  expectedPresenceGeneration: 0,
  groupName: "",
  modelName: "",
  protocol: "openai_images",
  taskMode: "unknown",
  idempotencyKeySupported: false,
  idempotencyHeader: "Idempotency-Key",
  taskIDReturned: false,
  taskIDJSONPath: "id",
  taskQuerySupported: false,
  taskQueryPathTemplate: "",
  taskStatusJSONPath: "status",
  taskSuccessValues: "completed,succeeded",
  taskFailureValues: "cancelled,failed",
  expectedVersion: 0,
}

export type ImageCapabilityStatus = "complete" | "confirmed" | "pending"
export type ImageCapabilityGroupMode = "channel" | "status"
export type ImageCapabilityStatusFilter = ImageCapabilityStatus | "all"
export type ImageCapabilityProtocolFilter = GatewayImageCapabilityProtocol | "all"
export type ImageCapabilityChannelFilter = `${number}` | "all"

export type ImageCapabilitySummary = {
  total: number
  channels: number
  pending: number
  confirmed: number
  complete: number
}

/**
 * A batch target deliberately contains only an identity that has already been
 * synchronized to the administrator UI, plus the version of the matching
 * capability record. It never carries inferred capability facts.
 */
export type ImageCapabilityBatchTarget = {
  key: string
  channelID: number
  channelName: string
  upstreamGroupID: number
  presenceGeneration: number
  groupName: string
  modelName: string
  inventorySnapshot: string
  existing?: GatewayImageOperationCapability
  expectedVersion: number
  action: "create" | "update"
  currentSnapshot: boolean
}

export type ImageCapabilityBatchFailureKind = "conflict" | "generation_changed" | "missing" | "cancelled" | "failed"

export type ImageCapabilityBatchFailure = {
  target: ImageCapabilityBatchTarget
  kind: ImageCapabilityBatchFailureKind
  reason: string
}

export type ImageCapabilityBatchResult<T> = {
  succeeded: Array<{ target: ImageCapabilityBatchTarget; value: T }>
  failed: ImageCapabilityBatchFailure[]
  cancelled: boolean
}

export type ImageCapabilityConfigInput = Pick<
  CapabilityForm,
  | "protocol"
  | "taskMode"
  | "idempotencyKeySupported"
  | "idempotencyHeader"
  | "taskIDReturned"
  | "taskIDJSONPath"
  | "taskQuerySupported"
  | "taskQueryPathTemplate"
  | "taskStatusJSONPath"
  | "taskSuccessValues"
  | "taskFailureValues"
>

export function imageCapabilityStatus(
  item: Pick<GatewayImageOperationCapability, "recovery_complete" | "recovery_blockers" | "task_mode">,
): ImageCapabilityStatus {
  const capabilityBlockers = item.recovery_blockers.filter((blocker) => blocker !== "gateway_task_recovery_not_implemented")
  if (item.recovery_complete || (item.task_mode !== "unknown" && capabilityBlockers.length === 0)) return "complete"
  if (item.task_mode !== "unknown") return "confirmed"
  return "pending"
}

export function imageCapabilityStatusLabel(status: ImageCapabilityStatus) {
  if (status === "complete") return "能力证据完整"
  if (status === "confirmed") return "模式已确认"
  return "模式待确认"
}

export function filterImageOperationCapabilities(
  items: GatewayImageOperationCapability[],
  search: string,
  status: ImageCapabilityStatusFilter,
  protocol: ImageCapabilityProtocolFilter = "all",
  channel: ImageCapabilityChannelFilter = "all",
) {
  const needle = search.trim().toLocaleLowerCase("zh-CN")
  return items
    .filter((item) => status === "all" || imageCapabilityStatus(item) === status)
    .filter((item) => protocol === "all" || item.protocol === protocol)
    .filter((item) => channel === "all" || String(item.channel_id) === channel)
    .filter((item) => {
      if (!needle) return true
      const haystack = [
        item.channel_name || `渠道 #${item.channel_id}`,
        String(item.channel_id),
        item.group_name,
        item.model_name,
        protocolLabel(item.protocol),
        item.protocol === "openai_images" ? "Images API 图片生成" : "Responses 图像工具",
        taskModeLabel(item.task_mode),
        evidenceLabel(item.evidence_source),
        imageCapabilityStatusLabel(imageCapabilityStatus(item)),
      ].join(" ").toLocaleLowerCase("zh-CN")
      return haystack.includes(needle)
    })
    .sort(compareImageCapabilities)
}

export function summarizeImageOperationCapabilities(
  items: GatewayImageOperationCapability[],
): ImageCapabilitySummary {
  const summary: ImageCapabilitySummary = {
    total: items.length,
    channels: 0,
    pending: 0,
    confirmed: 0,
    complete: 0,
  }
  const channels = new Set<number>()
  for (const item of items) {
    channels.add(item.channel_id)
    summary[imageCapabilityStatus(item)] += 1
  }
  summary.channels = channels.size
  return summary
}

export function imageCapabilityChannelOptions(items: GatewayImageOperationCapability[]) {
  const channels = new Map<number, { label: string; count: number }>()
  for (const item of items) {
    const current = channels.get(item.channel_id)
    if (current) {
      current.count += 1
      if (item.channel_name?.trim()) current.label = item.channel_name.trim()
    } else channels.set(item.channel_id, { label: item.channel_name?.trim() || `渠道 #${item.channel_id}`, count: 1 })
  }
  return Array.from(channels, ([id, channel]) => ({ value: String(id) as `${number}`, id, ...channel }))
    .sort((left, right) => {
      const labelOrder = left.label.localeCompare(right.label, "zh-CN", { numeric: true, sensitivity: "base" })
      return labelOrder !== 0 ? labelOrder : left.id - right.id
    })
}

export type ImageCapabilityBatchStatusFilter = ImageCapabilityStatus | "unconfigured" | "all"

export function imageCapabilityBatchTargets(
  inventory: GatewayImageOperationTarget[],
  capabilities: GatewayImageOperationCapability[],
  protocol: GatewayImageCapabilityProtocol,
  inventorySnapshot = "",
): ImageCapabilityBatchTarget[] {
  const existingByKey = new Map<string, GatewayImageOperationCapability>()
  for (const capability of capabilities) {
    if (capability.protocol !== protocol) continue
    existingByKey.set(imageCapabilityBatchIdentity(
      capability.channel_id,
      capability.upstream_group_id,
      capability.presence_generation,
      capability.model_name,
    ), capability)
  }

  const targets = new Map<string, ImageCapabilityBatchTarget>()
  for (const item of inventory) {
    const channelID = Number(item.channel_id)
    const upstreamGroupID = Number(item.upstream_group_id)
    const presenceGeneration = Number(item.presence_generation)
    const groupName = String(item.group_name ?? "").trim()
    const modelName = String(item.model_name ?? "").trim()
    const channelName = String(item.channel_name ?? "").trim() || `渠道 #${channelID}`
    if (!Number.isInteger(channelID) || channelID <= 0 || !Number.isInteger(upstreamGroupID) || upstreamGroupID <= 0 ||
      !Number.isInteger(presenceGeneration) || presenceGeneration <= 0 || !groupName || !modelName) continue
    const key = imageCapabilityBatchIdentity(channelID, upstreamGroupID, presenceGeneration, modelName)
    const existing = existingByKey.get(key)
    targets.set(key, {
      key,
      channelID,
      channelName,
      upstreamGroupID,
      presenceGeneration,
      groupName,
      modelName,
      inventorySnapshot,
      existing,
      expectedVersion: existing?.capability_version ?? 0,
      action: existing ? "update" : "create",
      currentSnapshot: true,
    })
  }

  // Current capability records are independently generation-scoped. Retain
  // records that are absent from this browser's candidate snapshot so a stale
  // or filtered probe response never makes an administrator silently lose a
  // valid edit target. The server still performs the generation fence on save.
  for (const capability of capabilities) {
    if (capability.protocol !== protocol) continue
    const key = imageCapabilityBatchIdentity(
      capability.channel_id,
      capability.upstream_group_id,
      capability.presence_generation,
      capability.model_name,
    )
    if (targets.has(key)) continue
    targets.set(key, {
      key,
      channelID: capability.channel_id,
      channelName: capability.channel_name?.trim() || `渠道 #${capability.channel_id}`,
      upstreamGroupID: capability.upstream_group_id,
      presenceGeneration: capability.presence_generation,
      groupName: capability.group_name,
      modelName: capability.model_name,
      inventorySnapshot,
      existing: capability,
      expectedVersion: capability.capability_version,
      action: "update",
      currentSnapshot: false,
    })
  }

  return Array.from(targets.values()).sort((left, right) => (
    left.channelName.localeCompare(right.channelName, "zh-CN", { numeric: true, sensitivity: "base" })
    || left.channelID - right.channelID
    || left.groupName.localeCompare(right.groupName, "zh-CN", { numeric: true, sensitivity: "base" })
    || left.modelName.localeCompare(right.modelName, "en-US", { numeric: true, sensitivity: "base" })
  ))
}

export function filterImageCapabilityBatchTargets(
  targets: ImageCapabilityBatchTarget[],
  search: string,
  status: ImageCapabilityBatchStatusFilter = "all",
  channel: ImageCapabilityChannelFilter = "all",
) {
  const needle = search.trim().toLocaleLowerCase("zh-CN")
  return targets.filter((target) => {
    const targetStatus = imageCapabilityBatchTargetStatus(target)
    if (status !== "all" && targetStatus !== status) return false
    if (channel !== "all" && String(target.channelID) !== channel) return false
    if (!needle) return true
    return [
      target.channelName,
      String(target.channelID),
      target.groupName,
      target.modelName,
      target.action === "create" ? "新建 尚无配置" : "更新 已有配置",
      imageCapabilityBatchStatusLabel(targetStatus),
    ].join(" ").toLocaleLowerCase("zh-CN").includes(needle)
  })
}

export function imageCapabilityBatchChannelOptions(targets: ImageCapabilityBatchTarget[]) {
  const channels = new Map<number, { label: string; count: number }>()
  for (const target of targets) {
    const current = channels.get(target.channelID)
    if (current) current.count += 1
    else channels.set(target.channelID, { label: target.channelName, count: 1 })
  }
  return Array.from(channels, ([id, channel]) => ({ value: String(id) as `${number}`, id, ...channel }))
    .sort((left, right) => (
      left.label.localeCompare(right.label, "zh-CN", { numeric: true, sensitivity: "base" }) || left.id - right.id
    ))
}

export function imageCapabilityBatchTargetStatus(target: ImageCapabilityBatchTarget): Exclude<ImageCapabilityBatchStatusFilter, "all"> {
  return target.existing ? imageCapabilityStatus(target.existing) : "unconfigured"
}

export function imageCapabilityBatchStatusLabel(status: Exclude<ImageCapabilityBatchStatusFilter, "all">) {
  return status === "unconfigured" ? "尚无当前协议配置" : imageCapabilityStatusLabel(status)
}

export function imageCapabilityBatchRequest(target: ImageCapabilityBatchTarget, form: ImageCapabilityConfigInput) {
  return {
    channel_id: target.channelID,
    expected_upstream_group_id: target.upstreamGroupID,
    expected_presence_generation: target.presenceGeneration,
    group_name: target.groupName.trim(),
    model_name: target.modelName.trim(),
    operation: "image_generation" as const,
    protocol: form.protocol,
    endpoint_path: form.protocol === "openai_images" ? "/v1/images/generations" : "/v1/responses",
    task_mode: form.taskMode,
    idempotency_key_supported: form.idempotencyKeySupported,
    idempotency_header: form.idempotencyKeySupported ? form.idempotencyHeader.trim() : "",
    task_id_returned: form.taskIDReturned,
    task_id_json_path: form.taskIDReturned ? form.taskIDJSONPath.trim() : "",
    task_query_supported: form.taskQuerySupported,
    task_query_method: form.taskQuerySupported ? "GET" : "",
    task_query_path_template: form.taskQuerySupported ? form.taskQueryPathTemplate.trim() : "",
    task_status_json_path: form.taskQuerySupported ? form.taskStatusJSONPath.trim() : "",
    task_success_values: form.taskQuerySupported ? commaValues(form.taskSuccessValues) : [],
    task_failure_values: form.taskQuerySupported ? commaValues(form.taskFailureValues) : [],
    expected_version: target.expectedVersion,
  }
}

export async function runImageCapabilityBatch<T>(
  targets: ImageCapabilityBatchTarget[],
  save: (target: ImageCapabilityBatchTarget, signal?: AbortSignal) => Promise<T>,
  options: { concurrency?: number; signal?: AbortSignal } = {},
): Promise<ImageCapabilityBatchResult<T>> {
  const concurrency = Math.max(1, Math.min(4, Math.floor(options.concurrency ?? 3)))
  const outcomes: Array<{ ok: true; value: T } | { ok: false; failure: ImageCapabilityBatchFailure } | undefined> = new Array(targets.length)
  let nextIndex = 0

  const worker = async () => {
    while (!options.signal?.aborted) {
      const index = nextIndex
      nextIndex += 1
      if (index >= targets.length) return
      const target = targets[index]
      try {
        outcomes[index] = { ok: true, value: await save(target, options.signal) }
      } catch (error) {
        outcomes[index] = { ok: false, failure: imageCapabilityBatchFailure(target, error, options.signal?.aborted === true) }
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, targets.length) }, () => worker()))
  for (let index = 0; index < targets.length; index += 1) {
    if (!outcomes[index]) {
      outcomes[index] = {
        ok: false,
        failure: { target: targets[index], kind: "cancelled", reason: "已取消，未向服务器发送该项配置" },
      }
    }
  }

  const succeeded: Array<{ target: ImageCapabilityBatchTarget; value: T }> = []
  const failed: ImageCapabilityBatchFailure[] = []
  for (let index = 0; index < targets.length; index += 1) {
    const outcome = outcomes[index]
    if (!outcome) continue
    if (outcome.ok) succeeded.push({ target: targets[index], value: outcome.value })
    else failed.push(outcome.failure)
  }
  return { succeeded, failed, cancelled: failed.some((failure) => failure.kind === "cancelled") }
}

export function imageCapabilityBatchFailure(target: ImageCapabilityBatchTarget, error: unknown, aborted = false): ImageCapabilityBatchFailure {
  if (aborted || (error instanceof Error && error.name === "AbortError")) {
    return { target, kind: "cancelled", reason: "已取消；此项未完成，结果未被假定为已保存" }
  }
  const message = error instanceof Error ? error.message.toLocaleLowerCase("en-US") : ""
  if (message.includes("version changed")) {
    return { target, kind: "conflict", reason: "能力版本已被其他操作更新；未覆盖对方修改，请刷新后重试" }
  }
  if (message.includes("upstream group changed")) {
    return { target, kind: "generation_changed", reason: "上游分组代次已变化；没有写入新代次，请刷新目标后重新确认" }
  }
  if (message.includes("not currently present")) {
    return { target, kind: "missing", reason: "对应上游分组当前不存在；没有创建或覆盖能力配置" }
  }
  return { target, kind: "failed", reason: "保存失败；此项未被标记为成功，可刷新后重试" }
}

export function imageCapabilityRetryableTargets<T>(result: ImageCapabilityBatchResult<T> | null) {
  return result?.failed
    .filter((failure) => failure.kind === "failed" || failure.kind === "cancelled")
    .map((failure) => failure.target) ?? []
}

function imageCapabilityBatchIdentity(channelID: number, upstreamGroupID: number, presenceGeneration: number, modelName: string) {
  return JSON.stringify([channelID, upstreamGroupID, presenceGeneration, modelName.trim()])
}

export function groupImageOperationCapabilities(
  items: GatewayImageOperationCapability[],
  mode: ImageCapabilityGroupMode,
) {
  const groups = new Map<string, { key: string; label: string; status?: ImageCapabilityStatus; items: GatewayImageOperationCapability[] }>()
  for (const item of items.slice().sort(compareImageCapabilities)) {
    const status = imageCapabilityStatus(item)
    const key = mode === "status" ? `status:${status}` : `channel:${item.channel_id}`
    const label = mode === "status" ? imageCapabilityStatusLabel(status) : item.channel_name || `渠道 #${item.channel_id}`
    const group = groups.get(key)
    if (group) group.items.push(item)
    else groups.set(key, { key, label, status: mode === "status" ? status : undefined, items: [item] })
  }
  const statusOrder: ImageCapabilityStatus[] = ["pending", "confirmed", "complete"]
  return Array.from(groups.values()).sort((left, right) => {
    if (mode === "status") return statusOrder.indexOf(left.status!) - statusOrder.indexOf(right.status!)
    const labelOrder = left.label.localeCompare(right.label, "zh-CN", { numeric: true, sensitivity: "base" })
    return labelOrder !== 0 ? labelOrder : left.key.localeCompare(right.key, "en-US", { numeric: true })
  })
}

export const IMAGE_CAPABILITY_RENDER_STEP = 200
export const IMAGE_CAPABILITY_RENDER_MAXIMUM = 400

export function boundedImageCapabilityGroupRows(
  groups: ReturnType<typeof groupImageOperationCapabilities>,
  collapsedGroups: ReadonlySet<string>,
  expandForSearch: boolean,
  requestedLimit = IMAGE_CAPABILITY_RENDER_STEP,
) {
  let remaining = Math.max(0, Math.min(IMAGE_CAPABILITY_RENDER_MAXIMUM, Math.floor(requestedLimit)))
  let hiddenExpandedItems = 0
  const boundedGroups = groups.map((group) => {
    const expanded = expandForSearch || !collapsedGroups.has(group.key)
    const visibleCount = expanded ? Math.min(group.items.length, remaining) : 0
    const visibleItems = expanded ? group.items.slice(0, visibleCount) : []
    if (expanded) {
      remaining -= visibleCount
      hiddenExpandedItems += group.items.length - visibleCount
    }
    return { ...group, expanded, visibleItems }
  })
  return {
    groups: boundedGroups,
    renderedItems: boundedGroups.reduce((count, group) => count + group.visibleItems.length, 0),
    hiddenExpandedItems,
  }
}

function compareImageCapabilities(left: GatewayImageOperationCapability, right: GatewayImageOperationCapability) {
  const channel = (left.channel_name || `渠道 #${left.channel_id}`).localeCompare(
    right.channel_name || `渠道 #${right.channel_id}`,
    "zh-CN",
    { numeric: true, sensitivity: "base" },
  )
  if (channel !== 0) return channel
  const group = left.group_name.localeCompare(right.group_name, "zh-CN", { numeric: true, sensitivity: "base" })
  if (group !== 0) return group
  const model = left.model_name.localeCompare(right.model_name, "zh-CN", { numeric: true, sensitivity: "base" })
  return model !== 0 ? model : left.id - right.id
}

export function ImageOperationCapabilitiesDialog() {
  const [open, setOpen] = useState(false)
  const [editing, setEditing] = useState(false)
  const [batchEditing, setBatchEditing] = useState(false)
  const [saving, setSaving] = useState(false)
  const [search, setSearch] = useState("")
  const [statusFilter, setStatusFilter] = useState<ImageCapabilityStatusFilter>("all")
  const [protocolFilter, setProtocolFilter] = useState<ImageCapabilityProtocolFilter>("all")
  const [channelFilter, setChannelFilter] = useState<ImageCapabilityChannelFilter>("all")
  const [groupMode, setGroupMode] = useState<ImageCapabilityGroupMode>("channel")
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(() => new Set())
  const [mainRenderLimit, setMainRenderLimit] = useState(IMAGE_CAPABILITY_RENDER_STEP)
  const [refreshRequested, setRefreshRequested] = useState(false)
  const [form, setForm] = useState<CapabilityForm>(emptyForm)
  const [targetSearch, setTargetSearch] = useState("")
  const [auditTarget, setAuditTarget] = useState<GatewayImageOperationCapability | null>(null)
  const [auditItems, setAuditItems] = useState<GatewayImageOperationCapabilityAudit[]>([])
  const [auditLoading, setAuditLoading] = useState(false)
  const [auditError, setAuditError] = useState("")
  const auditRequestID = useRef(0)
  const lastQueryError = useRef("")
  const refreshObservedBusy = useRef(false)
  const knownGroupKeys = useRef(new Set<string>())
  const query = useGatewayImageOperationCapabilities(open)
  const inventory = useGatewayImageOperationTargetInventory(open)
  const targetOptions = useMemo(() => imageCapabilityTargetOptions(inventory.data?.items ?? []), [inventory.data])
  const visibleTargetOptions = useMemo(
    () => filterImageCapabilityTargetOptions(targetOptions, targetSearch),
    [targetOptions, targetSearch],
  )
  const targetSelectOptions = useMemo(() => {
    const currentValue = targetValue(form)
    const currentTarget = targetOptions.find((option) => option.value === currentValue)
    const visible = visibleTargetOptions.slice(0, 200)
    return currentTarget && !visible.some((option) => option.value === currentTarget.value)
      ? [currentTarget, ...visible.slice(0, 199)]
      : visible
  }, [form, targetOptions, visibleTargetOptions])
  const summary = useMemo(() => summarizeImageOperationCapabilities(query.data ?? []), [query.data])
  const channelOptions = useMemo(() => imageCapabilityChannelOptions(query.data ?? []), [query.data])
  const filteredItems = useMemo(
    () => filterImageOperationCapabilities(query.data ?? [], search, statusFilter, protocolFilter, channelFilter),
    [channelFilter, protocolFilter, query.data, search, statusFilter],
  )
  const groupedItems = useMemo(
    () => groupImageOperationCapabilities(filteredItems, groupMode),
    [filteredItems, groupMode],
  )
  const effectiveCollapsedGroups = useMemo(() => {
    const next = new Set(collapsedGroups)
    for (const group of groupedItems) {
      if (!knownGroupKeys.current.has(group.key)) next.add(group.key)
    }
    return next
  }, [collapsedGroups, groupedItems])
  const boundedGroups = useMemo(
    () => boundedImageCapabilityGroupRows(groupedItems, effectiveCollapsedGroups, Boolean(search.trim()), mainRenderLimit),
    [effectiveCollapsedGroups, groupedItems, mainRenderLimit, search],
  )
  const allGroupsCollapsed = groupedItems.length > 0 && groupedItems.every((group) => effectiveCollapsedGroups.has(group.key))
  const filtersActive = Boolean(search.trim()) || statusFilter !== "all" || protocolFilter !== "all" || channelFilter !== "all"
  const inventoryComplete = inventory.fresh && inventory.data !== null && !inventory.loading && !inventory.refreshing && !inventory.error
  const writesReady = imageCapabilityWritesReady(
    inventory,
    query.loading,
    query.refreshing,
    query.error,
    query.data !== null,
  )
  const writeBlockedReason = imageCapabilityWriteBlockedReason(inventory, query.loading, query.refreshing, query.error, query.data !== null)
  const refreshBusy = query.loading || query.refreshing || inventory.loading || inventory.refreshing
  const canCreate = writesReady && targetOptions.length > 0

  useEffect(() => {
    if (!open) {
      auditRequestID.current += 1
      setEditing(false)
      setAuditTarget(null)
      setRefreshRequested(false)
      setTargetSearch("")
      refreshObservedBusy.current = false
      knownGroupKeys.current.clear()
      setCollapsedGroups(new Set())
      setMainRenderLimit(IMAGE_CAPABILITY_RENDER_STEP)
    }
  }, [open])

  useEffect(() => {
    if (!open || groupedItems.length === 0) return
    setCollapsedGroups((current) => {
      const next = new Set(current)
      let changed = false
      for (const group of groupedItems) {
        if (knownGroupKeys.current.has(group.key)) continue
        knownGroupKeys.current.add(group.key)
        next.add(group.key)
        changed = true
      }
      return changed ? next : current
    })
  }, [groupedItems, open])

  useEffect(() => {
    setMainRenderLimit(IMAGE_CAPABILITY_RENDER_STEP)
  }, [channelFilter, groupMode, protocolFilter, search, statusFilter])

  useEffect(() => {
    if (channelFilter === "all" || channelOptions.some((option) => option.value === channelFilter)) return
    setChannelFilter("all")
  }, [channelFilter, channelOptions])

  useEffect(() => {
    if (!open) return
    if (!query.error) {
      lastQueryError.current = ""
      return
    }
    if (refreshRequested || query.error === lastQueryError.current) return
    lastQueryError.current = query.error
    toast.error("加载生图操作能力失败，请重试")
  }, [open, query.error, refreshRequested])

  useEffect(() => {
    if (!refreshRequested) return
    if (query.loading || query.refreshing || inventory.loading || inventory.refreshing) {
      refreshObservedBusy.current = true
      return
    }
    if (!refreshObservedBusy.current) return
    refreshObservedBusy.current = false
    setRefreshRequested(false)
    if (query.error || inventory.error) {
      if (query.error) lastQueryError.current = query.error
      toast.error("刷新生图操作能力失败，请重试")
    }
    else toast.success("生图操作能力已刷新")
  }, [inventory.error, inventory.loading, inventory.refreshing, query.error, query.loading, query.refreshing, refreshRequested])

  function refreshCapabilities() {
    refreshObservedBusy.current = false
    setRefreshRequested(true)
    query.refetch()
    inventory.refetch()
  }

  function toggleGroup(key: string) {
    setCollapsedGroups((current) => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  function toggleAllGroups() {
    setCollapsedGroups(allGroupsCollapsed ? new Set() : new Set(groupedItems.map((group) => group.key)))
  }

  function clearCapabilityFilters() {
    setSearch("")
    setStatusFilter("all")
    setProtocolFilter("all")
    setChannelFilter("all")
  }

  function startCreate() {
    if (!canCreate) {
      toast.error(writesReady ? "当前目标清单没有已同步模型，请先刷新模型" : writeBlockedReason)
      return
    }
    const first = targetOptions[0]
    setForm(first ? capabilityFormForTarget(first) : emptyForm)
    setTargetSearch("")
    setEditing(true)
  }

  function startEdit(item: GatewayImageOperationCapability) {
    if (!writesReady) {
      toast.error(writeBlockedReason)
      return
    }
    setForm({
      channelID: String(item.channel_id),
      expectedUpstreamGroupID: item.upstream_group_id,
      expectedPresenceGeneration: item.presence_generation,
      groupName: item.group_name,
      modelName: item.model_name,
      protocol: item.protocol,
      taskMode: item.task_mode,
      idempotencyKeySupported: item.idempotency_key_supported,
      idempotencyHeader: item.idempotency_header || "Idempotency-Key",
      taskIDReturned: item.task_id_returned,
      taskIDJSONPath: item.task_id_json_path || "id",
      taskQuerySupported: item.task_query_supported,
      taskQueryPathTemplate: item.task_query_path_template || "",
      taskStatusJSONPath: item.task_status_json_path || "status",
      taskSuccessValues: item.task_success_values.join(","),
      taskFailureValues: item.task_failure_values.join(","),
      expectedVersion: item.capability_version,
    })
    setTargetSearch("")
    setEditing(true)
  }

  async function openAudit(item: GatewayImageOperationCapability) {
    const requestID = ++auditRequestID.current
    setAuditTarget(item)
    setAuditItems([])
    setAuditError("")
    setAuditLoading(true)
    try {
      const items = await apiFetch<GatewayImageOperationCapabilityAudit[]>(`/gateway/image-operation-capabilities/${item.id}/audit`)
      if (requestID === auditRequestID.current) setAuditItems(items)
    } catch {
      if (requestID === auditRequestID.current) {
        setAuditError("加载审计记录失败")
        toast.error("加载生图能力审计失败，请重试")
      }
    } finally {
      if (requestID === auditRequestID.current) setAuditLoading(false)
    }
  }

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!writesReady) {
      toast.error(writeBlockedReason)
      return
    }
    const validationError = capabilityFormError(form)
    if (validationError) {
      toast.error(validationError)
      return
    }
    setSaving(true)
    try {
      await apiFetch<GatewayImageOperationCapability>("/gateway/image-operation-capabilities", {
        method: "PATCH",
        body: JSON.stringify({
          channel_id: Number(form.channelID),
          expected_upstream_group_id: form.expectedUpstreamGroupID,
          expected_presence_generation: form.expectedPresenceGeneration,
          group_name: form.groupName.trim(),
          model_name: form.modelName.trim(),
          operation: "image_generation",
          protocol: form.protocol,
          endpoint_path: form.protocol === "openai_images" ? "/v1/images/generations" : "/v1/responses",
          task_mode: form.taskMode,
          idempotency_key_supported: form.idempotencyKeySupported,
          idempotency_header: form.idempotencyKeySupported ? form.idempotencyHeader.trim() : "",
          task_id_returned: form.taskIDReturned,
          task_id_json_path: form.taskIDReturned ? form.taskIDJSONPath.trim() : "",
          task_query_supported: form.taskQuerySupported,
          task_query_method: form.taskQuerySupported ? "GET" : "",
          task_query_path_template: form.taskQuerySupported ? form.taskQueryPathTemplate.trim() : "",
          task_status_json_path: form.taskQuerySupported ? form.taskStatusJSONPath.trim() : "",
          task_success_values: form.taskQuerySupported ? commaValues(form.taskSuccessValues) : [],
          task_failure_values: form.taskQuerySupported ? commaValues(form.taskFailureValues) : [],
          expected_version: form.expectedVersion,
        }),
      })
      toast.success("生图操作能力已保存")
      setEditing(false)
      query.refetch()
    } catch (error) {
      toast.error(capabilitySaveErrorMessage(error))
    } finally {
      setSaving(false)
    }
  }

  return (
    <>
      <Button type="button" size="sm" variant="outline" onClick={() => setOpen(true)}>
        <ImageIcon className="size-3.5" />
        {"生图能力"}
      </Button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="flex h-[min(48rem,calc(100vh-2rem))] w-[min(1320px,calc(100vw-2rem))] max-w-none flex-col gap-0 overflow-hidden p-0 sm:max-w-none">
          <DialogHeader className="border-b border-border bg-brand/5 px-5 py-4 pr-12">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <DialogTitle>{"生图操作能力"}</DialogTitle>
              <div className="flex items-center gap-2">
                <Badge variant="outline">{"仅管理员可见"}</Badge>
                <Badge variant="outline" title={inventory.data?.capturedAt ? `当前目标快照：${dateTime(inventory.data.capturedAt)}` : writeBlockedReason}>
                  {inventoryComplete ? "目标快照已就绪" : "目标快照不可写"}
                </Badge>
                <Button type="button" size="sm" variant="outline" disabled={refreshBusy} onClick={refreshCapabilities}>
                  <RefreshCw className={refreshBusy ? "size-3.5 animate-spin" : "size-3.5"} />
                  {refreshBusy ? "刷新中" : "刷新"}
                </Button>
                <Button type="button" size="sm" disabled={!canCreate} title={!canCreate ? (writesReady ? "当前目标清单没有已同步模型" : writeBlockedReason) : undefined} onClick={startCreate}>
                  <Plus className="size-3.5" />
                  {"显式配置"}
                </Button>
                <Button type="button" size="sm" variant="outline" disabled={!writesReady} title={!writesReady ? writeBlockedReason : undefined} onClick={() => setBatchEditing(true)}>
                  <ListChecks className="size-3.5" />
                  {"批量配置"}
                </Button>
              </div>
            </div>
          </DialogHeader>
          <div className="flex items-center gap-2 border-b border-warning/25 bg-warning/5 px-5 py-2.5 text-xs text-warning-foreground">
            <Tooltip>
              <TooltipTrigger asChild>
                <span role="img" aria-label="生图请求安全边界" className="inline-flex shrink-0 cursor-help">
                  <ShieldAlert aria-hidden="true" className="size-4" />
                </span>
              </TooltipTrigger>
              <TooltipContent>{"生图请求不会自动重放或跨渠道回退"}</TooltipContent>
            </Tooltip>
            <span>{"能力只来自成功真实请求、非付费结构化元数据或管理员配置。按次计费请求一旦物理派发可能已计费；配置不改账单或对账，自动重放与任务恢复始终关闭。"}</span>
          </div>
          {!writesReady ? (
            <div className={cn(
              "flex min-h-10 items-center justify-between gap-3 border-b px-5 py-2 text-xs",
              inventory.error || query.error ? "border-danger/20 bg-danger/5 text-destructive" : "border-border bg-muted/20 text-muted-foreground",
            )}>
              <span className="inline-flex min-w-0 items-center gap-2">
                {refreshBusy ? <RefreshCw aria-hidden="true" className="size-3.5 shrink-0 animate-spin" /> : <AlertTriangle aria-hidden="true" className="size-3.5 shrink-0" />}
                <span>{writeBlockedReason}</span>
              </span>
              {(inventory.error || query.error) && !refreshBusy ? <Button type="button" size="sm" variant="outline" onClick={refreshCapabilities}><RefreshCw aria-hidden="true" className="size-3.5" />{"重试"}</Button> : null}
            </div>
          ) : null}
          <div className="shrink-0 space-y-3 border-b border-border bg-background px-4 py-3">
            <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
              <CapabilityStatCard
                label="能力记录"
                value={summary.total}
                hint={`覆盖 ${summary.channels} 个渠道；点击清除全部筛选`}
                icon={ImageIcon}
                tone="brand"
                active={!filtersActive}
                disabled={query.loading && !query.data}
                onClick={clearCapabilityFilters}
              />
              <CapabilityStatCard
                label="模式待确认"
                value={summary.pending}
                hint="尚未获得可信的同步或异步任务模式证据"
                icon={AlertTriangle}
                tone="warning"
                active={statusFilter === "pending"}
                disabled={query.loading && !query.data}
                onClick={() => setStatusFilter("pending")}
              />
              <CapabilityStatCard
                label="模式已确认"
                value={summary.confirmed}
                hint="任务模式已有可信证据，但恢复条件仍不完整"
                icon={ShieldCheck}
                tone="brand"
                active={statusFilter === "confirmed"}
                disabled={query.loading && !query.data}
                onClick={() => setStatusFilter("confirmed")}
              />
              <CapabilityStatCard
                label="能力证据完整"
                value={summary.complete}
                hint="幂等、任务 ID、任务查询和终态映射证据完整；网关自动重放仍由安全边界独立关闭"
                icon={CircleCheck}
                tone="success"
                active={statusFilter === "complete"}
                disabled={query.loading && !query.data}
                onClick={() => setStatusFilter("complete")}
              />
            </div>
            <div className="grid grid-cols-2 gap-2 lg:grid-cols-[minmax(16rem,1fr)_minmax(11rem,14rem)_10rem_10rem_11rem_auto]">
              <div className="relative col-span-2 lg:col-span-1 lg:min-w-0">
                <Search aria-hidden="true" className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  aria-label="搜索生图操作能力"
                  className="pl-9"
                  placeholder="搜索渠道、渠道 ID、分组、模型或证据"
                  value={search}
                  onChange={(event) => setSearch(event.target.value)}
                />
              </div>
              <Select value={channelFilter} onValueChange={(value) => setChannelFilter(value as ImageCapabilityChannelFilter)}>
                <SelectTrigger className="w-full" aria-label="按渠道筛选"><SelectValue /></SelectTrigger>
                <SelectContent className="max-h-72">
                  <SelectItem value="all">{`全部渠道 (${summary.channels})`}</SelectItem>
                  {channelOptions.map((option) => (
                    <SelectItem key={option.value} value={option.value}>{`${option.label} (${option.count})`}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={protocolFilter} onValueChange={(value) => setProtocolFilter(value as ImageCapabilityProtocolFilter)}>
                <SelectTrigger className="w-full" aria-label="按生图协议筛选"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">{"全部协议"}</SelectItem>
                  <SelectItem value="openai_images">{"Images API"}</SelectItem>
                  <SelectItem value="openai_responses">{"Responses 工具"}</SelectItem>
                </SelectContent>
              </Select>
              <Select value={statusFilter} onValueChange={(value) => setStatusFilter(value as ImageCapabilityStatusFilter)}>
                <SelectTrigger className="w-full" aria-label="按能力状态筛选"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">{"全部状态"}</SelectItem>
                  <SelectItem value="pending">{`模式待确认 (${summary.pending})`}</SelectItem>
                  <SelectItem value="confirmed">{`模式已确认 (${summary.confirmed})`}</SelectItem>
                  <SelectItem value="complete">{`能力证据完整 (${summary.complete})`}</SelectItem>
                </SelectContent>
              </Select>
              <Select value={groupMode} onValueChange={(value) => setGroupMode(value as ImageCapabilityGroupMode)}>
                <SelectTrigger className="w-full" aria-label="生图能力分组方式"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="channel">{"按渠道分组"}</SelectItem>
                  <SelectItem value="status">{"按能力状态分组"}</SelectItem>
                </SelectContent>
              </Select>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button type="button" size="icon" variant="outline" disabled={groupedItems.length === 0} onClick={toggleAllGroups} aria-label={allGroupsCollapsed ? "展开全部分组" : "折叠全部分组"}>
                    <ChevronsDownUp aria-hidden="true" className="size-4" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent>{allGroupsCollapsed ? "展开全部分组" : "折叠全部分组"}</TooltipContent>
              </Tooltip>
            </div>
            <div className="flex min-h-7 flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
              <div className="flex flex-wrap items-center gap-2">
                <span className="tabular-nums">{filtersActive ? `显示 ${filteredItems.length} / ${summary.total} 项` : `共 ${summary.total} 项，${summary.channels} 个渠道`}</span>
                {filtersActive ? (
                  <Button type="button" size="sm" variant="ghost" className="h-7 gap-1 px-2 text-xs" onClick={clearCapabilityFilters}>
                    <X aria-hidden="true" className="size-3.5" />
                    {"清除筛选"}
                  </Button>
                ) : null}
              </div>
              {query.refreshing || inventory.refreshing ? <span className="inline-flex items-center gap-1.5"><RefreshCw aria-hidden="true" className="size-3.5 animate-spin" />{"正在刷新，当前数据仅供查看"}</span> : null}
              {query.error && query.data ? (
                <span className="inline-flex items-center gap-1.5 text-destructive">
                  <AlertTriangle aria-hidden="true" className="size-3.5" />
                  {"上次刷新失败，当前显示已有数据且暂停写入"}
                </span>
              ) : null}
            </div>
          </div>
          <div className="min-h-0 flex-1 overflow-auto">
            <Table noScroll className="min-w-[1160px] table-fixed">
              <TableHeader className="sticky top-0 z-10 bg-muted/95 backdrop-blur">
                <TableRow>
                  <TableHead className="w-[16%]">{"渠道 / 分组"}</TableHead>
                  <TableHead className="w-[18%]">{"模型"}</TableHead>
                  <TableHead className="w-[10%] text-center">{"协议"}</TableHead>
                  <TableHead className="w-[9%] text-center">{"模式"}</TableHead>
                  <TableHead className="w-[13%] text-center">{"幂等 / 查询"}</TableHead>
                  <TableHead className="w-[16%]">{"证据"}</TableHead>
                  <TableHead className="w-[11%] text-center">{"能力状态"}</TableHead>
                  <TableHead className="w-[7%] text-center">{"操作"}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {query.loading && !query.data ? <TableRow><TableCell colSpan={8} className="h-40 text-center text-muted-foreground"><span className="inline-flex items-center gap-2"><RefreshCw aria-hidden="true" className="size-4 animate-spin" />{"加载生图能力中"}</span></TableCell></TableRow> : null}
                {!query.loading && query.error && !query.data ? <TableRow><TableCell colSpan={8} className="h-40 text-center"><div className="flex flex-col items-center gap-3 text-destructive"><span>{"生图能力加载失败"}</span><Button type="button" size="sm" variant="outline" onClick={refreshCapabilities}><RefreshCw aria-hidden="true" className="size-3.5" />{"重试"}</Button></div></TableCell></TableRow> : null}
                {!query.loading && !query.error && (query.data?.length ?? 0) === 0 ? <TableRow><TableCell colSpan={8} className="h-40 text-center text-muted-foreground">{"暂无可信生图操作能力记录"}</TableCell></TableRow> : null}
                {(query.data?.length ?? 0) > 0 && filteredItems.length === 0 ? <TableRow><TableCell colSpan={8} className="h-40 text-center text-muted-foreground">{"没有匹配当前筛选条件的记录"}</TableCell></TableRow> : null}
                {boundedGroups.groups.map((group) => {
                  const collapsed = !group.expanded
                  return (
                    <Fragment key={group.key}>
                      <TableRow className="bg-muted/40 hover:bg-muted/50">
                        <TableCell colSpan={8} className="p-0">
                          <button
                            type="button"
                            className="flex h-10 w-full items-center gap-2 px-3 text-left text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset"
                            aria-label={`${collapsed ? "展开" : "折叠"}${group.label}分组`}
                            aria-expanded={!collapsed}
                            onClick={() => toggleGroup(group.key)}
                          >
                            {collapsed ? <ChevronRight aria-hidden="true" className="size-4 shrink-0" /> : <ChevronDown aria-hidden="true" className="size-4 shrink-0" />}
                            {group.status ? <CapabilityStatusIcon status={group.status} label={`${group.label}状态分组`} /> : null}
                            <span className="truncate font-semibold">{group.label}</span>
                            <Badge variant="secondary" className="shrink-0 tabular-nums">{group.items.length}</Badge>
                            <span className="ml-auto hidden shrink-0 text-xs text-muted-foreground sm:inline">{imageCapabilityGroupSummary(group.items)}</span>
                          </button>
                        </TableCell>
                      </TableRow>
                      {!collapsed ? group.visibleItems.map((item) => (
                        <ImageCapabilityRow key={item.id} item={item} editDisabled={!writesReady} editDisabledReason={writeBlockedReason} onAudit={openAudit} onEdit={startEdit} />
                      )) : null}
                    </Fragment>
                  )
                })}
                {boundedGroups.hiddenExpandedItems > 0 ? (
                  <TableRow>
                    <TableCell colSpan={8} className="h-12 text-center text-xs text-muted-foreground">
                      {mainRenderLimit < IMAGE_CAPABILITY_RENDER_MAXIMUM ? (
                        <Button type="button" size="sm" variant="outline" onClick={() => setMainRenderLimit((current) => Math.min(IMAGE_CAPABILITY_RENDER_MAXIMUM, current + IMAGE_CAPABILITY_RENDER_STEP))}>
                          {`继续显示 ${Math.min(IMAGE_CAPABILITY_RENDER_STEP, boundedGroups.hiddenExpandedItems)} 项`}
                        </Button>
                      ) : `仍有 ${boundedGroups.hiddenExpandedItems} 项未渲染，请缩小搜索范围或只展开需要的分组`}
                    </TableCell>
                  </TableRow>
                ) : null}
              </TableBody>
            </Table>
          </div>
        </DialogContent>
      </Dialog>
      <Dialog open={editing} onOpenChange={(next) => { if (!saving) setEditing(next) }}>
        <DialogContent className="max-h-[calc(100vh-2rem)] overflow-y-auto sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle className="pr-8">{form.expectedVersion > 0
              ? `${channelNameForID(query.data ?? [], Number(form.channelID))} / ${form.groupName} · ${form.modelName} · 生图能力配置`
              : "新增生图能力显式配置"}</DialogTitle>
          </DialogHeader>
          <form className="space-y-4" onSubmit={save}>
            {targetOptions.length > 0 && form.expectedVersion === 0 ? (
              <div className="space-y-1.5">
                <div className="text-xs font-medium text-muted-foreground">{"已同步目标"}</div>
                <div className="space-y-2 rounded-md border border-border bg-muted/15 p-3">
                  <div className="relative">
                    <Search aria-hidden="true" className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
                    <Input
                      value={targetSearch}
                      onChange={(event) => setTargetSearch(event.target.value)}
                      className="bg-background pl-9"
                      placeholder="先搜索渠道、分组或模型，再选择目标"
                      aria-label="搜索可配置的生图目标"
                    />
                  </div>
                  <Select value={targetValue(form)} onValueChange={(value) => {
                    const target = targetOptions.find((item) => item.value === value)
                    if (target) setForm((current) => ({
                      ...current,
                      channelID: String(target.channelID),
                      expectedUpstreamGroupID: target.upstreamGroupID,
                      expectedPresenceGeneration: target.presenceGeneration,
                      groupName: target.groupName,
                      modelName: target.modelName,
                    }))
                  }}>
                    <SelectTrigger className="w-full bg-background" aria-label="选择可配置的生图目标"><SelectValue placeholder="选择渠道、分组和模型" /></SelectTrigger>
                    <SelectContent className="max-h-72">
                      {targetSelectOptions.map((target) => <SelectItem key={target.value} value={target.value}>{target.label}</SelectItem>)}
                    </SelectContent>
                  </Select>
                  <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
                    <span>{targetSearch.trim()
                      ? visibleTargetOptions.length > 0
                        ? `匹配 ${visibleTargetOptions.length} / ${targetOptions.length} 个目标`
                        : "没有新的匹配项；下拉框仍保留当前已选目标"
                      : `共 ${targetOptions.length} 个已同步目标`}</span>
                    {visibleTargetOptions.length > 200 ? <span>{"列表先显示 200 项，请继续缩小搜索范围"}</span> : null}
                  </div>
                </div>
              </div>
            ) : null}
            <div className="grid gap-4 sm:grid-cols-3">
              <Field label="渠道 ID"><Input inputMode="numeric" value={form.channelID} disabled /></Field>
              <Field label="分组"><Input value={form.groupName} disabled /></Field>
              <Field label="模型"><Input value={form.modelName} disabled /></Field>
            </div>
            <div className="grid gap-4 sm:grid-cols-2">
              <Field label="调用协议"><Select value={form.protocol} disabled={form.expectedVersion > 0} onValueChange={(value) => setForm((current) => ({ ...current, protocol: value as GatewayImageCapabilityProtocol }))}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="openai_images">{"Images API"}</SelectItem><SelectItem value="openai_responses">{"Responses 图像工具"}</SelectItem></SelectContent></Select></Field>
              <Field label="任务返回方式"><Select value={form.taskMode} onValueChange={(value) => setForm((current) => ({ ...current, taskMode: value as GatewayImageTaskMode }))}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="unknown">{"尚未确认"}</SelectItem><SelectItem value="sync">{"同步返回结果"}</SelectItem><SelectItem value="async">{"异步返回任务"}</SelectItem><SelectItem value="sync_async">{"同时支持同步与异步"}</SelectItem></SelectContent></Select></Field>
            </div>
            <ToggleField label="上游接受幂等请求头" description="只记录已经确认的协议事实；不会因此开启自动重试或重放。" checked={form.idempotencyKeySupported} onCheckedChange={(checked) => setForm((current) => ({ ...current, idempotencyKeySupported: checked }))} />
            {form.idempotencyKeySupported ? <Field label="幂等请求头"><Input value={form.idempotencyHeader} onChange={(event) => setForm((current) => ({ ...current, idempotencyHeader: event.target.value }))} /></Field> : null}
            <ToggleField label="响应中返回任务 ID" description="用于标记异步任务身份；需要填写任务 ID 在 JSON 中的位置。" checked={form.taskIDReturned} onCheckedChange={(checked) => setForm((current) => ({ ...current, taskIDReturned: checked }))} />
            {form.taskIDReturned ? <Field label="任务 ID JSON 路径"><Input value={form.taskIDJSONPath} onChange={(event) => setForm((current) => ({ ...current, taskIDJSONPath: event.target.value }))} /></Field> : null}
            <ToggleField label="提供只读任务查询接口" description="记录如何查询任务状态；当前版本仍不会自动恢复或重放任务。" checked={form.taskQuerySupported} onCheckedChange={(checked) => setForm((current) => ({ ...current, taskQuerySupported: checked }))} />
            {form.taskQuerySupported ? <div className="grid gap-4 sm:grid-cols-2"><Field label="GET 路径模板"><Input placeholder="/v1/tasks/{task_id}" value={form.taskQueryPathTemplate} onChange={(event) => setForm((current) => ({ ...current, taskQueryPathTemplate: event.target.value }))} /></Field><Field label="状态 JSON 路径"><Input value={form.taskStatusJSONPath} onChange={(event) => setForm((current) => ({ ...current, taskStatusJSONPath: event.target.value }))} /></Field><Field label="成功状态（逗号分隔）"><Input value={form.taskSuccessValues} onChange={(event) => setForm((current) => ({ ...current, taskSuccessValues: event.target.value }))} /></Field><Field label="失败状态（逗号分隔）"><Input value={form.taskFailureValues} onChange={(event) => setForm((current) => ({ ...current, taskFailureValues: event.target.value }))} /></Field></div> : null}
            <div className="rounded-md border border-warning/25 bg-warning/5 px-3 py-2 text-xs text-muted-foreground">{"保存这些描述不会开启重试、自动重放或任务恢复，也不会改写账单或对账；网关仍按付费非幂等请求失败关闭。"}</div>
            <div className="flex justify-end gap-2 border-t border-border pt-4"><Button type="button" variant="outline" disabled={saving} onClick={() => setEditing(false)}>{"取消"}</Button><Button type="submit" disabled={saving || !writesReady} title={!writesReady ? writeBlockedReason : undefined}>{saving ? <><RefreshCw aria-hidden="true" className="size-3.5 animate-spin" />{"保存中"}</> : "保存配置"}</Button></div>
          </form>
        </DialogContent>
      </Dialog>
      <ImageCapabilityBatchDialog
        open={batchEditing}
        inventory={inventory}
        capabilities={query.data ?? []}
        writesReady={writesReady}
        writeBlockedReason={writeBlockedReason}
        onOpenChange={(next) => setBatchEditing(next)}
        onSaved={(saved) => {
          if (saved.length === 0) return
          query.setData((current) => mergeImageCapabilityBatchResults(current ?? [], saved))
        }}
      />
      <Dialog open={auditTarget !== null} onOpenChange={(next) => { if (!next) { auditRequestID.current += 1; setAuditTarget(null) } }}>
        <DialogContent className="flex h-[min(38rem,calc(100vh-2rem))] w-[min(860px,calc(100vw-2rem))] max-w-none flex-col gap-0 overflow-hidden p-0 sm:max-w-none">
          <DialogHeader className="border-b border-border px-5 py-4 pr-12">
            <DialogTitle>{auditTarget ? `${auditTarget.channel_name || `渠道 #${auditTarget.channel_id}`} / ${auditTarget.group_name} · ${auditTarget.model_name} · 能力审计` : "生图能力审计"}</DialogTitle>
          </DialogHeader>
          <div className="min-h-0 flex-1 overflow-auto">
            <Table className="min-w-[760px] table-fixed">
              <TableHeader className="sticky top-0 z-10 bg-muted/95 backdrop-blur"><TableRow><TableHead className="w-[19%]">{"观察时间"}</TableHead><TableHead className="w-[15%] text-center">{"动作"}</TableHead><TableHead className="w-[20%] text-center">{"证据来源"}</TableHead><TableHead className="w-[12%] text-center">{"版本"}</TableHead><TableHead className="w-[17%]">{"执行者"}</TableHead><TableHead className="w-[17%]">{"引用"}</TableHead></TableRow></TableHeader>
              <TableBody>
                {auditLoading ? <TableRow><TableCell colSpan={6} className="h-40 text-center text-muted-foreground"><span className="inline-flex items-center gap-2"><RefreshCw aria-hidden="true" className="size-4 animate-spin" />{"加载审计记录中"}</span></TableCell></TableRow> : null}
                {!auditLoading && auditError ? <TableRow><TableCell colSpan={6} className="h-40 text-center"><div className="flex flex-col items-center gap-3 text-destructive"><span>{"审计记录加载失败"}</span>{auditTarget ? <Button type="button" size="sm" variant="outline" onClick={() => void openAudit(auditTarget)}><RefreshCw aria-hidden="true" className="size-3.5" />{"重试"}</Button> : null}</div></TableCell></TableRow> : null}
                {!auditLoading && !auditError && auditItems.length === 0 ? <TableRow><TableCell colSpan={6} className="h-40 text-center text-muted-foreground">{"暂无审计记录"}</TableCell></TableRow> : null}
                {auditItems.map((item) => <TableRow key={item.id}><TableCell className="text-xs">{dateTime(item.evidence_observed_at)}</TableCell><TableCell className="text-center"><Badge variant="outline">{auditActionLabel(item.action)}</Badge></TableCell><TableCell className="text-center text-xs">{evidenceLabel(item.evidence_source)}</TableCell><TableCell className="text-center font-mono text-xs">{item.capability_version}</TableCell><TableCell><span className="block truncate text-xs" title={item.actor}>{item.actor}</span></TableCell><TableCell className="text-xs">{item.evidence_reference_kind === "request_log" && item.evidence_reference_id ? `请求 #${item.evidence_reference_id}` : "—"}</TableCell></TableRow>)}
              </TableBody>
            </Table>
          </div>
        </DialogContent>
      </Dialog>
    </>
  )
}

function ImageCapabilityBatchDialog({
  open,
  onOpenChange,
  inventory,
  capabilities,
  writesReady,
  writeBlockedReason,
  onSaved,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  inventory: GatewayImageOperationTargetInventoryState
  capabilities: GatewayImageOperationCapability[]
  writesReady: boolean
  writeBlockedReason: string
  onSaved: (saved: GatewayImageOperationCapability[]) => void
}) {
  const [form, setForm] = useState<ImageCapabilityConfigInput>(() => imageCapabilityConfigFromForm(emptyForm))
  const [search, setSearch] = useState("")
  const [status, setStatus] = useState<ImageCapabilityBatchStatusFilter>("all")
  const [channel, setChannel] = useState<ImageCapabilityChannelFilter>("all")
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(() => new Set())
  const [collapsedChannels, setCollapsedChannels] = useState<Set<number>>(() => new Set())
  const [batchRenderLimit, setBatchRenderLimit] = useState(IMAGE_CAPABILITY_RENDER_STEP)
  const [pendingTargets, setPendingTargets] = useState<{ targets: ImageCapabilityBatchTarget[]; form: ImageCapabilityConfigInput; inventorySnapshot: string } | null>(null)
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState<ImageCapabilityBatchResult<GatewayImageOperationCapability> | null>(null)
  const [resultForm, setResultForm] = useState<ImageCapabilityConfigInput | null>(null)
  const controllerRef = useRef<AbortController | null>(null)
  const runningRef = useRef(false)
  const runningSnapshotRef = useRef("")
  const inventorySnapshot = inventory.data?.snapshot ?? ""
  const batchWritable = writesReady && inventory.fresh && inventorySnapshot !== ""
  const allTargets = useMemo(
    () => imageCapabilityBatchTargets(inventory.data?.items ?? [], capabilities, form.protocol, inventorySnapshot),
    [capabilities, form.protocol, inventory.data, inventorySnapshot],
  )
  const visibleTargets = useMemo(
    () => filterImageCapabilityBatchTargets(allTargets, search, status, channel),
    [allTargets, channel, search, status],
  )
  const channelOptions = useMemo(() => imageCapabilityBatchChannelOptions(allTargets), [allTargets])
  const targetsByChannel = useMemo(() => groupImageCapabilityBatchTargets(visibleTargets), [visibleTargets])
  const boundedTargetGroups = useMemo(
    () => boundedImageCapabilityBatchGroupRows(targetsByChannel, collapsedChannels, Boolean(search.trim()), batchRenderLimit),
    [batchRenderLimit, collapsedChannels, search, targetsByChannel],
  )
  const selectedTargets = useMemo(
    () => allTargets.filter((target) => selectedKeys.has(target.key)),
    [allTargets, selectedKeys],
  )
  const selectedVisibleCount = visibleTargets.filter((target) => selectedKeys.has(target.key)).length
  const allVisibleSelected = visibleTargets.length > 0 && selectedVisibleCount === visibleTargets.length
  const validationError = imageCapabilityConfigError(form)

  useEffect(() => {
    if (!open) {
      controllerRef.current?.abort()
      controllerRef.current = null
      runningRef.current = false
      runningSnapshotRef.current = ""
      setPendingTargets(null)
      setRunning(false)
      return
    }
    setSelectedKeys(new Set())
    setCollapsedChannels(new Set(allTargets.map((target) => target.channelID)))
    setResult(null)
    setResultForm(null)
    setBatchRenderLimit(IMAGE_CAPABILITY_RENDER_STEP)
  // Only reset the batch workbench when it is explicitly opened. Candidate
  // polling must not silently clear a selection while an operator is working.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  useEffect(() => {
    setBatchRenderLimit(IMAGE_CAPABILITY_RENDER_STEP)
  }, [channel, search, status])

  useEffect(() => {
    const snapshotChanged = runningSnapshotRef.current !== "" && runningSnapshotRef.current !== inventorySnapshot
    if (runningRef.current && (!batchWritable || snapshotChanged)) controllerRef.current?.abort()
    if (pendingTargets && (!batchWritable || pendingTargets.inventorySnapshot !== inventorySnapshot)) setPendingTargets(null)
  }, [batchWritable, inventorySnapshot, pendingTargets])

  useEffect(() => {
    const available = new Set(allTargets.map((target) => target.key))
    setSelectedKeys((current) => {
      const next = new Set(Array.from(current).filter((key) => available.has(key)))
      return next.size === current.size ? current : next
    })
    if (channel !== "all" && !channelOptions.some((option) => option.value === channel)) setChannel("all")
  }, [allTargets, channel, channelOptions])

  function toggleTarget(target: ImageCapabilityBatchTarget, checked: boolean) {
    setSelectedKeys((current) => {
      const next = new Set(current)
      if (checked) next.add(target.key)
      else next.delete(target.key)
      return next
    })
  }

  function toggleVisible(checked: boolean) {
    setSelectedKeys((current) => {
      const next = new Set(current)
      for (const target of visibleTargets) {
        if (checked) next.add(target.key)
        else next.delete(target.key)
      }
      return next
    })
  }

  function toggleChannelTargets(channelID: number, checked: boolean) {
    const targets = targetsByChannel.find((group) => group.channelID === channelID)?.targets ?? []
    setSelectedKeys((current) => {
      const next = new Set(current)
      for (const target of targets) {
        if (checked) next.add(target.key)
        else next.delete(target.key)
      }
      return next
    })
  }

  function requestApply() {
    if (!batchWritable) {
      toast.error(writeBlockedReason)
      return
    }
    if (validationError) {
      toast.error(validationError)
      return
    }
    if (selectedTargets.length === 0) {
      toast.error("请先选择至少一个已同步模型目标")
      return
    }
    setPendingTargets({ targets: selectedTargets, form: { ...form }, inventorySnapshot })
  }

  async function applyTargets(targets: ImageCapabilityBatchTarget[], config: ImageCapabilityConfigInput, expectedInventorySnapshot: string) {
    // React state does not synchronously disable a double click. This ref is
    // the dispatch gate: without it two confirm events could create two
    // batches and violate the three-request bound before the rerender lands.
    if (targets.length === 0 || runningRef.current) return
    if (!batchWritable || expectedInventorySnapshot === "" || expectedInventorySnapshot !== inventorySnapshot ||
      targets.some((target) => target.inventorySnapshot !== expectedInventorySnapshot)) {
      toast.error("目标快照已变化，请刷新并重新确认后再批量应用")
      return
    }
    runningRef.current = true
    runningSnapshotRef.current = expectedInventorySnapshot
    const controller = new AbortController()
    controllerRef.current = controller
    setRunning(true)
    setResult(null)
    setResultForm(config)
    try {
      const next = await runImageCapabilityBatch(
        targets,
        (target, signal) => apiFetch<GatewayImageOperationCapability>("/gateway/image-operation-capabilities", {
          method: "PATCH",
          signal,
          body: JSON.stringify(imageCapabilityBatchRequest(target, config)),
        }),
        { concurrency: 3, signal: controller.signal },
      )
      setResult(next)
      onSaved(next.succeeded.map((entry) => entry.value))
      setSelectedKeys(new Set(next.failed.map((failure) => failure.target.key)))
      if (next.failed.length > 0) {
        toast.warning(`已保存 ${next.succeeded.length} 项，${next.failed.length} 项未完成，请查看逐项结果`)
      } else {
        toast.success(`已批量保存 ${next.succeeded.length} 项生图能力配置`)
      }
    } finally {
      controllerRef.current = null
      runningRef.current = false
      runningSnapshotRef.current = ""
      setRunning(false)
    }
  }

  function retryRetryable() {
    const targets = imageCapabilityRetryableTargets(result)
    if (targets.length === 0) return
    void applyTargets(targets, resultForm ?? form, targets[0]?.inventorySnapshot ?? "")
  }

  const createCount = pendingTargets?.targets.filter((target) => target.action === "create").length ?? 0
  const updateCount = pendingTargets?.targets.length ? pendingTargets.targets.length - createCount : 0
  const retryableFailureCount = result?.failed.filter((failure) => failure.kind === "failed" || failure.kind === "cancelled").length ?? 0

  return (
    <>
      <Dialog open={open} onOpenChange={(next) => { if (!running) onOpenChange(next) }}>
        <DialogContent className="flex h-[min(52rem,calc(100vh-2rem))] w-[min(1380px,calc(100vw-2rem))] max-w-none flex-col gap-0 overflow-hidden p-0 sm:max-w-none">
          <DialogHeader className="border-b border-border bg-brand/5 px-5 py-4 pr-12">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="flex min-w-0 items-center gap-2"><span className="flex size-9 shrink-0 items-center justify-center rounded-md bg-brand/10 text-brand"><ListChecks aria-hidden="true" className="size-4" /></span><DialogTitle>{"批量配置生图能力"}</DialogTitle></div>
              <Badge variant="outline" title={inventory.data?.capturedAt ? `目标快照：${dateTime(inventory.data.capturedAt)}` : writeBlockedReason}>{"每项保留独立版本与代次校验"}</Badge>
            </div>
          </DialogHeader>
          {!batchWritable ? <div className="flex min-h-10 items-center gap-2 border-b border-danger/20 bg-danger/5 px-4 py-2 text-xs text-destructive"><AlertTriangle aria-hidden="true" className="size-3.5 shrink-0" /><span>{writeBlockedReason}</span></div> : null}
          <div className="grid min-h-0 flex-1 lg:grid-cols-[minmax(0,1fr)_23rem]">
            <section className="flex min-h-0 min-w-0 flex-col border-b border-border lg:border-b-0 lg:border-r">
              <div className="space-y-2 border-b border-border bg-muted/10 p-3">
                <div className="grid gap-2 sm:grid-cols-[minmax(14rem,1fr)_11rem_11rem]">
                  <div className="relative"><Search aria-hidden="true" className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" /><Input value={search} onChange={(event) => setSearch(event.target.value)} disabled={running} className="pl-9" aria-label="搜索批量生图目标" placeholder="搜索渠道、分组或模型" /></div>
                  <Select value={channel} onValueChange={(value) => setChannel(value as ImageCapabilityChannelFilter)} disabled={running}><SelectTrigger aria-label="筛选批量目标渠道"><SelectValue /></SelectTrigger><SelectContent className="max-h-72"><SelectItem value="all">{`全部渠道 (${channelOptions.length})`}</SelectItem>{channelOptions.map((option) => <SelectItem key={option.value} value={option.value}>{`${option.label} (${option.count})`}</SelectItem>)}</SelectContent></Select>
                  <Select value={status} onValueChange={(value) => setStatus(value as ImageCapabilityBatchStatusFilter)} disabled={running}><SelectTrigger aria-label="筛选批量目标状态"><SelectValue /></SelectTrigger><SelectContent><SelectItem value="all">{"全部状态"}</SelectItem><SelectItem value="unconfigured">{"尚无当前协议配置"}</SelectItem><SelectItem value="pending">{"模式待确认"}</SelectItem><SelectItem value="confirmed">{"模式已确认"}</SelectItem><SelectItem value="complete">{"能力证据完整"}</SelectItem></SelectContent></Select>
                </div>
                <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
                  <label className="flex cursor-pointer items-center gap-2"><Checkbox checked={allVisibleSelected} onCheckedChange={(checked) => toggleVisible(checked === true)} disabled={running || !batchWritable || visibleTargets.length === 0} aria-label="选择当前筛选结果" /><span>{`选择当前筛选结果 ${selectedVisibleCount} / ${visibleTargets.length}`}</span></label>
                  <span>{`已选 ${selectedTargets.length} 项；当前协议 ${protocolLabel(form.protocol)}`}</span>
                </div>
              </div>
              <div className="min-h-0 flex-1 overflow-y-auto p-3">
                {targetsByChannel.length === 0 ? <div className="flex h-36 items-center justify-center text-sm text-muted-foreground">{"没有匹配的已同步模型目标"}</div> : <div className="space-y-2">{boundedTargetGroups.groups.map((group) => {
                  const collapsed = !group.expanded
                  const selectedCount = group.targets.filter((target) => selectedKeys.has(target.key)).length
                  const allSelected = group.targets.length > 0 && selectedCount === group.targets.length
                  return <section key={group.channelID} className="overflow-hidden rounded-md border border-border bg-background">
                    <div className="flex min-h-10 items-center gap-2 bg-muted/35 px-3">
                      <Checkbox checked={allSelected} onCheckedChange={(checked) => toggleChannelTargets(group.channelID, checked === true)} disabled={running || !batchWritable} aria-label={`选择渠道 ${group.channelName} 的当前筛选目标`} />
                      <button type="button" className="flex min-w-0 flex-1 items-center gap-2 py-2 text-left text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring" onClick={() => setCollapsedChannels((current) => { const next = new Set(current); if (next.has(group.channelID)) next.delete(group.channelID); else next.add(group.channelID); return next })} aria-expanded={!collapsed} aria-label={`${collapsed ? "展开" : "折叠"}渠道 ${group.channelName}`}>
                        {collapsed ? <ChevronRight aria-hidden="true" className="size-4 shrink-0" /> : <ChevronDown aria-hidden="true" className="size-4 shrink-0" />}<span className="truncate font-medium">{group.channelName}</span><Badge variant="secondary" className="shrink-0 tabular-nums">{group.targets.length}</Badge><span className="ml-auto shrink-0 text-xs text-muted-foreground">{`${selectedCount} 已选`}</span>
                      </button>
                    </div>
                    {!collapsed ? <ul className="divide-y divide-border/70">{group.visibleTargets.map((target) => <li key={target.key} className="grid grid-cols-[1rem_minmax(0,1fr)_auto] items-center gap-2 px-3 py-2"><Checkbox checked={selectedKeys.has(target.key)} onCheckedChange={(checked) => toggleTarget(target, checked === true)} disabled={running || !batchWritable} aria-label={`选择 ${target.channelName} ${target.groupName} ${target.modelName}`} /><div className="min-w-0"><div className="flex min-w-0 items-center gap-1.5"><span className="truncate text-sm" title={target.groupName}>{target.groupName}</span><Badge variant={target.action === "create" ? "outline" : "secondary"} className="h-5 shrink-0 px-1.5 text-[10px]">{target.action === "create" ? "新建" : "更新"}</Badge>{!target.currentSnapshot ? <Badge variant="outline" className="h-5 shrink-0 px-1.5 text-[10px]">{"当前快照未见"}</Badge> : null}</div><code className="block truncate text-xs text-muted-foreground" title={target.modelName}>{target.modelName}</code></div><Badge variant="outline" className="max-w-28 truncate text-[10px]" title={imageCapabilityBatchStatusLabel(imageCapabilityBatchTargetStatus(target))}>{imageCapabilityBatchStatusLabel(imageCapabilityBatchTargetStatus(target))}</Badge></li>)}</ul> : null}
                  </section>
                })}{boundedTargetGroups.hiddenExpandedItems > 0 ? <div className="flex min-h-11 items-center justify-center rounded-md border border-dashed border-border px-3 text-xs text-muted-foreground">{batchRenderLimit < IMAGE_CAPABILITY_RENDER_MAXIMUM ? <Button type="button" size="sm" variant="outline" onClick={() => setBatchRenderLimit((current) => Math.min(IMAGE_CAPABILITY_RENDER_MAXIMUM, current + IMAGE_CAPABILITY_RENDER_STEP))}>{`继续显示 ${Math.min(IMAGE_CAPABILITY_RENDER_STEP, boundedTargetGroups.hiddenExpandedItems)} 项`}</Button> : `仍有 ${boundedTargetGroups.hiddenExpandedItems} 项未渲染，请继续缩小搜索范围`}</div> : null}</div>}
              </div>
            </section>
            <form className="min-h-0 overflow-y-auto bg-background p-4" onSubmit={(event) => { event.preventDefault(); requestApply() }}>
              <div className="space-y-4">
                <div><h3 className="text-sm font-semibold">{"应用的能力描述"}</h3><p className="mt-1 text-xs leading-5 text-muted-foreground">{"这些字段只作为管理员显式配置写入选中目标；不会根据名称推断能力，也不会开启重放、恢复或付费探测。按次计费请求一旦物理派发可能已计费，配置不改变账单或后续对账。"}</p></div>
                <Field label="调用协议"><Select value={form.protocol} disabled={running} onValueChange={(value) => setForm((current) => ({ ...current, protocol: value as GatewayImageCapabilityProtocol }))}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="openai_images">{"Images API"}</SelectItem><SelectItem value="openai_responses">{"Responses 图像工具"}</SelectItem></SelectContent></Select></Field>
                <Field label="任务返回方式"><Select value={form.taskMode} disabled={running} onValueChange={(value) => setForm((current) => ({ ...current, taskMode: value as GatewayImageTaskMode }))}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="unknown">{"尚未确认"}</SelectItem><SelectItem value="sync">{"同步返回结果"}</SelectItem><SelectItem value="async">{"异步返回任务"}</SelectItem><SelectItem value="sync_async">{"同时支持同步与异步"}</SelectItem></SelectContent></Select></Field>
                <ToggleField label="上游接受幂等请求头" description="只记录已确认的协议事实，不开启自动重放。" checked={form.idempotencyKeySupported} onCheckedChange={(checked) => setForm((current) => ({ ...current, idempotencyKeySupported: checked }))} />
                {form.idempotencyKeySupported ? <Field label="幂等请求头"><Input disabled={running} value={form.idempotencyHeader} onChange={(event) => setForm((current) => ({ ...current, idempotencyHeader: event.target.value }))} /></Field> : null}
                <ToggleField label="响应中返回任务 ID" description="仅记录 JSON 中任务 ID 的位置。" checked={form.taskIDReturned} onCheckedChange={(checked) => setForm((current) => ({ ...current, taskIDReturned: checked }))} />
                {form.taskIDReturned ? <Field label="任务 ID JSON 路径"><Input disabled={running} value={form.taskIDJSONPath} onChange={(event) => setForm((current) => ({ ...current, taskIDJSONPath: event.target.value }))} /></Field> : null}
                <ToggleField label="提供只读任务查询接口" description="记录查询事实；当前版本仍不会自动恢复任务。" checked={form.taskQuerySupported} onCheckedChange={(checked) => setForm((current) => ({ ...current, taskQuerySupported: checked }))} />
                {form.taskQuerySupported ? <div className="space-y-3"><Field label="GET 路径模板"><Input disabled={running} placeholder="/v1/tasks/{task_id}" value={form.taskQueryPathTemplate} onChange={(event) => setForm((current) => ({ ...current, taskQueryPathTemplate: event.target.value }))} /></Field><Field label="状态 JSON 路径"><Input disabled={running} value={form.taskStatusJSONPath} onChange={(event) => setForm((current) => ({ ...current, taskStatusJSONPath: event.target.value }))} /></Field><Field label="成功状态（逗号分隔）"><Input disabled={running} value={form.taskSuccessValues} onChange={(event) => setForm((current) => ({ ...current, taskSuccessValues: event.target.value }))} /></Field><Field label="失败状态（逗号分隔）"><Input disabled={running} value={form.taskFailureValues} onChange={(event) => setForm((current) => ({ ...current, taskFailureValues: event.target.value }))} /></Field></div> : null}
                <div className="sticky bottom-0 space-y-2 border-t border-border bg-background/95 pt-3 backdrop-blur"><div className="flex items-center justify-between text-xs"><span className="text-muted-foreground">{"待应用目标"}</span><span className="font-semibold tabular-nums">{selectedTargets.length}</span></div>{running ? <Button type="button" className="w-full" variant="outline" onClick={() => controllerRef.current?.abort()}><X aria-hidden="true" className="size-3.5" />{"取消未完成项"}</Button> : <Button type="submit" className="w-full" disabled={!batchWritable || selectedTargets.length === 0} title={!batchWritable ? writeBlockedReason : undefined}><ListChecks aria-hidden="true" className="size-3.5" />{`预览并应用 ${selectedTargets.length} 项`}</Button>}<p className="text-[11px] leading-4 text-muted-foreground">{"最多同时提交 3 项；每项独立 CAS，版本或分组代次冲突不会被覆盖。"}</p></div>
              </div>
            </form>
          </div>
          {result ? <ImageCapabilityBatchResultPanel result={result} retryableFailureCount={retryableFailureCount} running={running || !batchWritable} onRetry={retryRetryable} /> : null}
        </DialogContent>
      </Dialog>
      <AlertDialog open={pendingTargets !== null} onOpenChange={(next) => { if (!next && !running) setPendingTargets(null) }}>
        <AlertDialogContent>
          <AlertDialogHeader><AlertDialogTitle>{"确认批量应用生图能力配置"}</AlertDialogTitle><AlertDialogDescription>{`将向 ${pendingTargets?.targets.length ?? 0} 个已选目标分别提交当前管理员配置。每项均使用其独立版本，不会覆盖冲突或新代次。`}</AlertDialogDescription></AlertDialogHeader>
          {pendingTargets ? <div className="rounded-md border border-border bg-muted/15 p-3 text-sm"><div className="flex justify-between gap-3"><span className="text-muted-foreground">{"新建 / 更新"}</span><span className="font-semibold tabular-nums">{`${createCount} / ${updateCount}`}</span></div><div className="mt-1 text-xs text-muted-foreground">{`协议：${protocolLabel(pendingTargets.form.protocol)}；任务方式：${taskModeLabel(pendingTargets.form.taskMode)}`}</div><ul className="mt-2 max-h-36 space-y-1 overflow-y-auto text-xs">{pendingTargets.targets.slice(0, 8).map((target) => <li key={target.key} className="truncate" title={imageCapabilityBatchTargetLabel(target)}>{imageCapabilityBatchTargetLabel(target)}</li>)}{pendingTargets.targets.length > 8 ? <li className="text-muted-foreground">{`另有 ${pendingTargets.targets.length - 8} 个目标`}</li> : null}</ul></div> : null}
          <AlertDialogFooter><AlertDialogCancel disabled={running}>{"返回调整"}</AlertDialogCancel><AlertDialogAction disabled={running || !batchWritable || pendingTargets?.inventorySnapshot !== inventorySnapshot} onClick={() => { const pending = pendingTargets; setPendingTargets(null); if (pending) void applyTargets(pending.targets, pending.form, pending.inventorySnapshot) }}>{"确认应用"}</AlertDialogAction></AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}

function ImageCapabilityBatchResultPanel({ result, retryableFailureCount, running, onRetry }: { result: ImageCapabilityBatchResult<GatewayImageOperationCapability>; retryableFailureCount: number; running: boolean; onRetry: () => void }) {
  return <section className={result.failed.length > 0 ? "border-t border-warning/25 bg-warning/5 px-4 py-3" : "border-t border-success/25 bg-success/5 px-4 py-3"} aria-live="polite"><div className="flex flex-wrap items-center justify-between gap-2"><div className="flex items-center gap-2 text-sm"><Badge variant={result.failed.length > 0 ? "outline" : "default"}>{`成功 ${result.succeeded.length}`}</Badge>{result.failed.length > 0 ? <Badge variant="outline" className="border-warning/35 text-warning">{`未完成 ${result.failed.length}`}</Badge> : <span className="text-success">{"全部目标已保存"}</span>}</div>{retryableFailureCount > 0 ? <Button type="button" size="sm" variant="outline" disabled={running} onClick={onRetry}><RotateCcw aria-hidden="true" className="size-3.5" />{`重试未完成 ${retryableFailureCount} 项`}</Button> : null}</div>{result.failed.length > 0 ? <div className="mt-2 max-h-32 space-y-1.5 overflow-y-auto pr-1">{result.failed.map((failure) => <div key={`${failure.target.key}-${failure.kind}`} className="grid grid-cols-[minmax(0,1fr)_auto] items-start gap-3 rounded-md border border-warning/20 bg-background/80 px-2 py-1.5 text-xs"><span className="min-w-0"><span className="block truncate font-medium" title={imageCapabilityBatchTargetLabel(failure.target)}>{imageCapabilityBatchTargetLabel(failure.target)}</span><span className="block text-muted-foreground">{failure.reason}</span></span><Badge variant="outline" className="shrink-0 text-[10px]">{imageCapabilityBatchFailureLabel(failure.kind)}</Badge></div>)}</div> : null}{result.failed.some((failure) => failure.kind === "conflict" || failure.kind === "generation_changed") ? <p className="mt-2 text-[11px] leading-4 text-muted-foreground">{"版本或代次冲突需要刷新目标后重新确认，不会自动重试或覆盖。"}</p> : null}</section>
}

export function groupImageCapabilityBatchTargets(targets: ImageCapabilityBatchTarget[]) {
  const groups = new Map<number, { channelID: number; channelName: string; targets: ImageCapabilityBatchTarget[] }>()
  for (const target of targets) {
    const group = groups.get(target.channelID)
    if (group) group.targets.push(target)
    else groups.set(target.channelID, { channelID: target.channelID, channelName: target.channelName, targets: [target] })
  }
  return Array.from(groups.values())
}

export function boundedImageCapabilityBatchGroupRows(
  groups: ReturnType<typeof groupImageCapabilityBatchTargets>,
  collapsedChannels: ReadonlySet<number>,
  expandForSearch: boolean,
  requestedLimit = IMAGE_CAPABILITY_RENDER_STEP,
) {
  let remaining = Math.max(0, Math.min(IMAGE_CAPABILITY_RENDER_MAXIMUM, Math.floor(requestedLimit)))
  let hiddenExpandedItems = 0
  const boundedGroups = groups.map((group) => {
    const expanded = expandForSearch || !collapsedChannels.has(group.channelID)
    const visibleCount = expanded ? Math.min(group.targets.length, remaining) : 0
    const visibleTargets = expanded ? group.targets.slice(0, visibleCount) : []
    if (expanded) {
      remaining -= visibleCount
      hiddenExpandedItems += group.targets.length - visibleCount
    }
    return { ...group, expanded, visibleTargets }
  })
  return {
    groups: boundedGroups,
    renderedItems: boundedGroups.reduce((count, group) => count + group.visibleTargets.length, 0),
    hiddenExpandedItems,
  }
}

function imageCapabilityBatchTargetLabel(target: ImageCapabilityBatchTarget) {
  return `${target.channelName} / ${target.groupName} · ${target.modelName}`
}

function imageCapabilityBatchFailureLabel(kind: ImageCapabilityBatchFailureKind) {
  if (kind === "conflict") return "版本冲突"
  if (kind === "generation_changed") return "代次变化"
  if (kind === "missing") return "目标不存在"
  if (kind === "cancelled") return "已取消"
  return "保存失败"
}

function imageCapabilityConfigFromForm(form: CapabilityForm): ImageCapabilityConfigInput {
  return {
    protocol: form.protocol,
    taskMode: form.taskMode,
    idempotencyKeySupported: form.idempotencyKeySupported,
    idempotencyHeader: form.idempotencyHeader,
    taskIDReturned: form.taskIDReturned,
    taskIDJSONPath: form.taskIDJSONPath,
    taskQuerySupported: form.taskQuerySupported,
    taskQueryPathTemplate: form.taskQueryPathTemplate,
    taskStatusJSONPath: form.taskStatusJSONPath,
    taskSuccessValues: form.taskSuccessValues,
    taskFailureValues: form.taskFailureValues,
  }
}

function mergeImageCapabilityBatchResults(current: GatewayImageOperationCapability[], saved: GatewayImageOperationCapability[]) {
  const byID = new Map(current.map((item) => [item.id, item]))
  for (const item of saved) byID.set(item.id, item)
  return Array.from(byID.values()).sort(compareImageCapabilities)
}

function CapabilityStatCard({
  label,
  value,
  hint,
  icon: Icon,
  tone,
  active,
  disabled,
  onClick,
}: {
  label: string
  value: number
  hint: string
  icon: LucideIcon
  tone: "brand" | "success" | "warning"
  active: boolean
  disabled: boolean
  onClick: () => void
}) {
  const surfaceClass = tone === "success"
    ? "metric-surface-success border-success/20"
    : tone === "warning"
      ? "metric-surface-warning border-warning/25"
      : "metric-surface-brand border-brand/20"
  const iconClass = tone === "success"
    ? "bg-success/10 text-success"
    : tone === "warning"
      ? "bg-warning/10 text-warning"
      : "bg-brand/10 text-brand"
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          disabled={disabled}
          onClick={onClick}
          className={cn(
            "flex min-h-16 min-w-0 items-center gap-3 rounded-lg border px-3 py-2 text-left shadow-sm outline-none transition-[border-color,box-shadow,transform] hover:-translate-y-px hover:shadow-md focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50",
            surfaceClass,
            active && "ring-2 ring-brand/30",
          )}
          aria-label={`${label} ${value}。${hint}`}
          aria-pressed={active}
        >
          <span className={cn("flex size-9 shrink-0 items-center justify-center rounded-lg", iconClass)}>
            <Icon aria-hidden="true" className="size-4" />
          </span>
          <span className="min-w-0">
            <span className="block truncate text-xs font-medium text-muted-foreground">{label}</span>
            <span className="mt-0.5 block truncate text-2xl font-semibold leading-none tabular-nums">{value}</span>
          </span>
        </button>
      </TooltipTrigger>
      <TooltipContent className="max-w-72 text-xs">{hint}</TooltipContent>
    </Tooltip>
  )
}

function ImageCapabilityRow({
  item,
  editDisabled,
  editDisabledReason,
  onAudit,
  onEdit,
}: {
  item: GatewayImageOperationCapability
  editDisabled: boolean
  editDisabledReason: string
  onAudit: (item: GatewayImageOperationCapability) => Promise<void>
  onEdit: (item: GatewayImageOperationCapability) => void
}) {
  const status = imageCapabilityStatus(item)
  const statusLabel = imageCapabilityStatusLabel(status)
  const recoveryDetail = item.recovery_complete
    ? "任务恢复链所需能力证据完整"
    : item.recovery_blockers.map(recoveryBlockerLabel).join("；") || "任务恢复执行链尚未开放"
  return (
    <TableRow>
      <TableCell>
        <div className="min-w-0">
          <div className="truncate font-medium" title={item.channel_name || String(item.channel_id)}>{item.channel_name || `渠道 #${item.channel_id}`}</div>
          <div className="truncate text-xs text-muted-foreground" title={item.group_name}>{item.group_name}</div>
        </div>
      </TableCell>
      <TableCell><code className="block truncate text-xs" title={item.model_name}>{item.model_name}</code></TableCell>
      <TableCell className="text-center"><Badge variant="outline">{protocolLabel(item.protocol)}</Badge></TableCell>
      <TableCell className="text-center"><Badge variant="secondary">{taskModeLabel(item.task_mode)}</Badge></TableCell>
      <TableCell className="text-center text-xs">
        <div className="space-y-0.5">
          <div>{item.idempotency_key_supported ? "幂等已确认" : "幂等未知"}</div>
          <div className="text-muted-foreground">{item.task_query_supported ? "任务可查询" : "查询未知"}</div>
        </div>
      </TableCell>
      <TableCell>
        <div className="truncate text-xs" title={evidenceLabel(item.evidence_source)}>{evidenceLabel(item.evidence_source)}</div>
        <div className="truncate text-xs text-muted-foreground" title={dateTime(item.evidence_observed_at)}>{dateTime(item.evidence_observed_at)}</div>
      </TableCell>
      <TableCell className="text-center">
        <div className="flex flex-col items-center gap-1">
          <Badge variant={status === "complete" ? "default" : status === "confirmed" ? "secondary" : "outline"} className="gap-1">
            <CapabilityStatusIcon status={status} label={imageCapabilityStatusDescription(status)} />
            {statusLabel}
          </Badge>
          <Tooltip>
            <TooltipTrigger asChild>
              <Badge asChild variant="outline" className="cursor-help font-normal">
                <button type="button" aria-label={`自动恢复状态：${item.recovery_complete ? "恢复链完整" : "关闭"}。${recoveryDetail}`}>
                  {item.recovery_complete ? "恢复链完整" : "自动恢复关闭"}
                </button>
              </Badge>
            </TooltipTrigger>
            <TooltipContent className="max-w-80 text-xs">{recoveryDetail}</TooltipContent>
          </Tooltip>
        </div>
      </TableCell>
      <TableCell className="text-center">
        <div className="flex items-center justify-center gap-0.5">
          <Tooltip>
            <TooltipTrigger asChild>
              <Button type="button" size="icon-sm" variant="ghost" onClick={() => void onAudit(item)} aria-label={`查看 ${item.model_name} 生图能力审计`}>
                <History aria-hidden="true" className="size-3.5" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>{"查看可信证据审计"}</TooltipContent>
          </Tooltip>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button type="button" size="icon-sm" variant="ghost" disabled={editDisabled} title={editDisabled ? editDisabledReason : undefined} onClick={() => onEdit(item)} aria-label={`编辑 ${item.model_name} 生图能力`}>
                <Pencil aria-hidden="true" className="size-3.5" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>{"编辑这一项的显式配置"}</TooltipContent>
          </Tooltip>
        </div>
      </TableCell>
    </TableRow>
  )
}

function CapabilityStatusIcon({ status, label }: { status: ImageCapabilityStatus; label: string }) {
  const Icon = status === "complete" ? CircleCheck : status === "confirmed" ? ShieldCheck : AlertTriangle
  const color = status === "complete" ? "text-success" : status === "confirmed" ? "text-brand" : "text-warning-foreground"
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span role="img" aria-label={label} className={`inline-flex shrink-0 cursor-help ${color}`}>
          <Icon aria-hidden="true" className="size-3.5" />
        </span>
      </TooltipTrigger>
      <TooltipContent className="max-w-72 text-xs">{label}</TooltipContent>
    </Tooltip>
  )
}

function imageCapabilityStatusDescription(status: ImageCapabilityStatus) {
  if (status === "complete") return "任务恢复链所需能力证据完整"
  if (status === "confirmed") return "同步或异步任务模式已有可信证据，恢复仍受阻塞项限制"
  return "任务模式尚未确认"
}

function imageCapabilityGroupSummary(items: GatewayImageOperationCapability[]) {
  const counts: Record<ImageCapabilityStatus, number> = { pending: 0, confirmed: 0, complete: 0 }
  for (const item of items) counts[imageCapabilityStatus(item)] += 1
  return `模式待确认 ${counts.pending} · 模式已确认 ${counts.confirmed} · 能力证据完整 ${counts.complete}`
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return <label className="space-y-1.5"><span className="text-xs font-medium text-muted-foreground">{label}</span>{children}</label>
}

function ToggleField({ label, description, checked, onCheckedChange }: { label: string; description: string; checked: boolean; onCheckedChange: (checked: boolean) => void }) {
  return (
    <div className="flex items-center justify-between gap-4 rounded-md border border-border px-3 py-2.5">
      <span className="min-w-0">
        <span className="block text-sm font-medium">{label}</span>
        <span className="mt-0.5 block text-xs leading-5 text-muted-foreground">{description}</span>
      </span>
      <Switch checked={checked} onCheckedChange={onCheckedChange} aria-label={label} />
    </div>
  )
}

export function capabilityFormError(form: CapabilityForm) {
  const channelID = Number(form.channelID)
  if (!Number.isInteger(channelID) || channelID <= 0) return "请选择有效的渠道"
  if (!Number.isInteger(form.expectedUpstreamGroupID) || form.expectedUpstreamGroupID <= 0 ||
    !Number.isInteger(form.expectedPresenceGeneration) || form.expectedPresenceGeneration <= 0) {
    return "请选择带有当前代次的已同步目标"
  }
  if (!form.groupName.trim()) return "请填写上游分组"
  if (!form.modelName.trim()) return "请填写模型名称"
  return imageCapabilityConfigError(form)
}

export function imageCapabilityConfigError(form: ImageCapabilityConfigInput) {
  if (form.idempotencyKeySupported && !form.idempotencyHeader.trim()) return "请填写幂等请求头"
  if (form.taskIDReturned && !form.taskIDJSONPath.trim()) return "请填写任务 ID JSON 路径"
  if (!form.taskQuerySupported) return ""
  if (!form.taskQueryPathTemplate.trim().startsWith("/") || !form.taskQueryPathTemplate.includes("{task_id}")) {
    return "任务查询路径必须是包含 {task_id} 的相对路径"
  }
  if (!form.taskStatusJSONPath.trim()) return "请填写任务状态 JSON 路径"
  const successValues = commaValues(form.taskSuccessValues)
  const failureValues = commaValues(form.taskFailureValues)
  const failureSet = new Set(failureValues.map((value) => value.toLocaleLowerCase("en-US")))
  if (successValues.some((value) => failureSet.has(value.toLocaleLowerCase("en-US")))) return "同一个任务状态不能同时属于成功和失败"
  return ""
}

function capabilitySaveErrorMessage(error: unknown) {
  const message = error instanceof Error ? error.message.toLocaleLowerCase("en-US") : ""
  if (message.includes("version changed")) return "这项能力已被其他操作更新，请刷新列表后重新编辑"
  if (message.includes("upstream group changed")) return "上游分组已发生变化，请刷新列表后重新配置"
  if (message.includes("not currently present")) return "对应的上游分组已不存在，不能继续保存"
  return "保存生图操作能力失败，请检查配置后重试"
}

function imageCapabilityWriteBlockedReason(
  inventory: GatewayImageOperationTargetInventoryState,
  capabilityLoading: boolean,
  capabilityRefreshing: boolean,
  capabilityError: string | null,
  hasCapabilityData: boolean,
) {
  if (inventory.loading || inventory.refreshing) return "正在加载当前代次目标，完成前仅可查看能力记录"
  if (inventory.error) return "当前代次目标加载失败，请重试；为避免使用不完整清单，配置写入已暂停"
  if (!inventory.fresh || !inventory.data) return "当前代次目标快照不完整，配置写入已暂停"
  if (capabilityLoading || capabilityRefreshing) return "正在加载最新能力版本，完成前仅可查看记录"
  if (capabilityError) return "能力版本加载失败，请重试；配置写入已暂停"
  if (!hasCapabilityData) return "能力版本尚未加载完成，配置写入已暂停"
  return "当前数据不可写，请刷新后重试"
}

export function imageCapabilityWritesReady(
  inventory: Pick<GatewayImageOperationTargetInventoryState, "data" | "loading" | "refreshing" | "error" | "fresh">,
  capabilityLoading: boolean,
  capabilityRefreshing: boolean,
  capabilityError: string | null,
  hasCapabilityData: boolean,
) {
  return inventory.fresh && inventory.data !== null && !inventory.loading && !inventory.refreshing && !inventory.error &&
    hasCapabilityData && !capabilityLoading && !capabilityRefreshing && !capabilityError
}

function commaValues(value: string) {
  return Array.from(new Set(value.split(",").map((item) => item.trim()).filter(Boolean)))
}

function protocolLabel(protocol: GatewayImageCapabilityProtocol) {
  return protocol === "openai_images" ? "Images" : "Responses"
}

function taskModeLabel(mode: GatewayImageTaskMode) {
  return mode === "sync" ? "同步" : mode === "async" ? "异步" : mode === "sync_async" ? "同步 + 异步" : "未知"
}

function evidenceLabel(source: GatewayImageOperationCapability["evidence_source"]) {
  if (source === "observed_success") return "成功真实请求"
  if (source === "provider_metadata") return "非付费结构化元数据"
  return "管理员配置"
}

function auditActionLabel(action: string) {
  if (action === "configured") return "显式配置"
  if (action === "provider_metadata") return "元数据"
  if (action === "evidence_ignored") return "证据忽略"
  return "真实观察"
}

function recoveryBlockerLabel(blocker: string) {
  if (blocker === "async_task_mode_not_confirmed") return "未确认异步任务模式"
  if (blocker === "idempotency_key_not_confirmed") return "未确认幂等键"
  if (blocker === "task_id_not_confirmed") return "未确认任务 ID"
  if (blocker === "task_query_not_confirmed") return "未确认任务查询"
  if (blocker === "terminal_state_mapping_incomplete") return "终态映射不完整"
  if (blocker === "gateway_task_recovery_not_implemented") return "网关任务恢复执行链尚未实现"
  return "存在未识别的恢复阻塞项"
}

export type ImageCapabilityTargetOption = {
  value: string
  label: string
  channelID: number
  upstreamGroupID: number
  presenceGeneration: number
  groupName: string
  modelName: string
}

function imageCapabilityTargetOptions(inventory: GatewayImageOperationTarget[]) {
  const seen = new Set<string>()
  const options: ImageCapabilityTargetOption[] = []
  for (const item of inventory) {
    const modelName = String(item.model_name ?? "").trim()
    const groupName = String(item.group_name ?? "").trim()
    const channelName = String(item.channel_name ?? "").trim() || `渠道 #${item.channel_id}`
    if (!modelName || !groupName || !item.channel_id || !item.upstream_group_id || !item.presence_generation) continue
    const value = targetIdentity(String(item.channel_id), item.upstream_group_id, item.presence_generation, modelName)
    if (seen.has(value)) continue
    seen.add(value)
    options.push({
      value,
      label: `${channelName} / ${groupName} · ${modelName}`,
      channelID: item.channel_id,
      upstreamGroupID: item.upstream_group_id,
      presenceGeneration: item.presence_generation,
      groupName,
      modelName,
    })
  }
  return options.sort((left, right) => left.label.localeCompare(right.label, "zh-CN"))
}

export function filterImageCapabilityTargetOptions(options: ImageCapabilityTargetOption[], search: string) {
  const needle = search.trim().toLocaleLowerCase("zh-CN")
  if (!needle) return options
  return options.filter((option) => (
    `${option.label} ${option.channelID} ${option.groupName} ${option.modelName}`
      .toLocaleLowerCase("zh-CN")
      .includes(needle)
  ))
}

function channelNameForID(items: GatewayImageOperationCapability[], channelID: number) {
  if (!Number.isInteger(channelID) || channelID <= 0) return "未知渠道"
  return items.find((item) => item.channel_id === channelID)?.channel_name?.trim() || `渠道 #${channelID}`
}

function targetValue(form: CapabilityForm) {
  return form.channelID && form.expectedUpstreamGroupID > 0 && form.expectedPresenceGeneration > 0 && form.modelName
    ? targetIdentity(form.channelID, form.expectedUpstreamGroupID, form.expectedPresenceGeneration, form.modelName)
    : ""
}

function targetIdentity(channelID: string, upstreamGroupID: number, presenceGeneration: number, modelName: string) {
  return JSON.stringify([channelID, upstreamGroupID, presenceGeneration, modelName])
}

function capabilityFormForTarget(target: ImageCapabilityTargetOption): CapabilityForm {
  return {
    ...emptyForm,
    channelID: String(target.channelID),
    expectedUpstreamGroupID: target.upstreamGroupID,
    expectedPresenceGeneration: target.presenceGeneration,
    groupName: target.groupName,
    modelName: target.modelName,
  }
}
