import type { GatewayUpstreamGroupChange } from "@/lib/api-types"

export const upstreamGroupChangeFilterAll = "__all__"

export type UpstreamGroupChangeView = "pending" | "review" | "history"

export type UpstreamGroupChangePendingFacet = "all" | "actionable" | "awaiting" | "attention"

export type UpstreamGroupChangeFilters = {
  search: string
  channel: string
  group: string
  source: string
}

export type UpstreamGroupChangeFilterOption = {
  value: string
  label: string
}

export type UpstreamGroupChangeFilterOptions = {
  channels: UpstreamGroupChangeFilterOption[]
  groups: UpstreamGroupChangeFilterOption[]
  sources: UpstreamGroupChangeFilterOption[]
}

export type UpstreamGroupChangeStatusGroup = {
  key: string
  channelName: string
  status: string
  statusLabel: string
  sourceNames: string[]
  changes: GatewayUpstreamGroupChange[]
}

export type UpstreamGroupChangeSourceGroup = {
  key: string
  sourceName: string
  changes: GatewayUpstreamGroupChange[]
}

export type UpstreamGroupChangeChannelGroup = {
  key: string
  channelID: number
  channelName: string
  changes: GatewayUpstreamGroupChange[]
  sources: UpstreamGroupChangeSourceGroup[]
}

export type UpstreamGroupChangePendingFacetCounts = Record<UpstreamGroupChangePendingFacet, number>

export function upstreamGroupChangeHistoryPath(view: Exclude<UpstreamGroupChangeView, "pending">, cursor: string | null | undefined, pageSize: number, filters: UpstreamGroupChangeFilters) {
  const params = new URLSearchParams({ page_size: String(Math.min(100, Math.max(1, Math.trunc(pageSize) || 50))) })
  if (view === "review") params.set("view", "review")
  const normalizedCursor = String(cursor ?? "").trim()
  if (normalizedCursor) params.set("cursor", normalizedCursor)
  const search = filters.search.trim()
  if (search) params.set("search", search)
  if (filters.channel !== upstreamGroupChangeFilterAll) params.set("channel", filters.channel)
  if (filters.group !== upstreamGroupChangeFilterAll) params.set("group", filters.group)
  if (filters.source !== upstreamGroupChangeFilterAll) params.set("source", filters.source)
  return `/gateway/upstream-group-changes/history?${params.toString()}`
}

const assignmentReasonLabels: Record<string, string> = {
  models_not_synced_for_generation: "当前分组代尚未完成模型同步；仍可人工加入，自动归组会等待事实",
  facts_incomplete: "模型或协议事实尚不完整",
  identity_conflict: "分组身份事实存在冲突",
  unclassified_models: "仍有模型无法归类",
  ambiguous_models: "模型归类结果存在歧义",
  no_rule_matched: "没有自动归组规则命中",
  equal_priority_targets: "同优先级规则指向多个候选分组",
  override_target_deleted: "人工指定的候选分组已不存在",
  group_generation_changed: "上游分组代已变化，需要重新判定",
  duplicate_pending_identity: "同一上游分组代的重复待处理项已合并",
  unique_highest_priority_match: "唯一最高优先级规则已命中",
  manual_ignore_override: "已由管理员明确忽略",
  manual_assign_override: "已由管理员明确指定候选分组",
  manual_override_incomplete_facts: "管理员已在事实不完整时明确指定候选分组",
}

const removalManualReviewReasonLabels: Record<string, string> = {
  manual_review_required: "受管密钥删除需要人工复核",
  remote_identity_changed: "上游密钥身份已发生变化",
  revalidation_required: "删除证据需要重新核验",
  removal_evidence_changed: "受管密钥删除证据已发生变化",
  channel_identity_changed: "渠道上游账户身份已发生变化",
  group_generation_changed: "上游分组代已发生变化",
  remote_identity_reappeared: "已处理的上游密钥身份重新出现",
  removal_claim_epoch_invalid: "删除处理代次无效",
  removal_intent_state_changed: "删除处理状态已发生变化",
  legacy_removal_intent: "旧版删除记录缺少当前安全栅栏",
  inventory_removal_intent_invalid: "上游密钥库存删除记录不完整",
  inventory_removal_identity_invalid: "上游密钥库存删除身份无效",
  inventory_removal_claim_invalid: "上游密钥库存删除声明无效",
  inventory_removal_evidence_invalid: "上游密钥库存删除证据无效",
  inventory_removal_fence_changed: "上游密钥库存删除栅栏已发生变化",
  remote_name_ambiguous: "上游存在多个同名密钥，无法确认删除目标",
  remote_name_reappeared: "同名上游密钥以新身份重新出现",
}

export function upstreamGroupChangeView(change: GatewayUpstreamGroupChange): UpstreamGroupChangeView {
  if (change.status === "pending") return "pending"
  if (upstreamGroupChangeRequiresManualRemovalReview(change)) return "review"
  return "history"
}

export function upstreamGroupChangeChannelLabel(change: GatewayUpstreamGroupChange) {
  return String(change.channel_name ?? "").trim() || `渠道 #${change.channel_id}`
}

export function upstreamGroupChangeSourceValue(change: GatewayUpstreamGroupChange) {
  if (change.upstream_key_id) return `key:${change.upstream_key_id}`
  const name = String(change.upstream_key_name ?? "").trim()
  if (name) return `name:${name}`
  return "unknown"
}

export function upstreamGroupChangeSourceLabel(change: GatewayUpstreamGroupChange) {
  const name = String(change.upstream_key_name ?? "").trim()
  if (name) return name
  if (change.upstream_key_id) return `Key #${change.upstream_key_id}`
  return "未标记来源"
}

export function upstreamGroupChangeReasonLabel(change: GatewayUpstreamGroupChange) {
  const blockedReason = String(change.assignment_blocked_reason ?? "").trim()
  if (blockedReason) return blockedReason
  const code = String(change.assignment_reason_code ?? "").trim()
  if (code && assignmentReasonLabels[code]) return assignmentReasonLabels[code]
  if (code) return code
  if (change.assignment_state === "awaiting_facts") return "模型与协议事实尚未补齐；仍可人工加入，自动归组会等待事实"
  if (change.assignment_state === "conflict") return "自动归组结果存在冲突"
  if (change.assignment_state === "unmatched") return "没有自动归组规则命中"
  if (change.assignment_state === "ready") return "已具备明确的归组结果"
  return "暂无自动归组说明"
}

export function upstreamGroupChangeIsActionable(change: GatewayUpstreamGroupChange) {
  return change.assignment_actionable !== false
}

export function upstreamGroupChangeRequiresManualRemovalReview(change: GatewayUpstreamGroupChange) {
  return change.kind === "removed"
    && change.status === "ignored"
    && String(change.assignment_reason_code ?? "").trim() === "manual_review_required"
    && !change.removal_manual_review_resolved_at
}

export function upstreamGroupChangeManualReviewReasonLabel(change: GatewayUpstreamGroupChange) {
  const reason = String(change.removal_manual_review_reason ?? "").trim()
  if (reason && removalManualReviewReasonLabels[reason]) return removalManualReviewReasonLabels[reason]
  if (reason) return "受管密钥状态需要人工核验"
  return "受管密钥删除需要人工复核"
}

export function upstreamGroupChangeAssignmentStateLabel(state: string | null | undefined) {
  if (state === "ready") return "可自动归组"
  if (state === "conflict") return "规则冲突"
  if (state === "awaiting_facts") return "等待事实"
  if (state === "unmatched") return "未命中规则"
  if (state === "assigned") return "已加入候选分组"
  if (state === "ignored") return "已忽略"
  if (state === "superseded") return "已过期"
  return "待人工处理"
}

export function groupUpstreamGroupChangesByChannelAndStatus(changes: GatewayUpstreamGroupChange[]): UpstreamGroupChangeStatusGroup[] {
  const groups = new Map<string, UpstreamGroupChangeStatusGroup>()
  for (const change of changes) {
    const status = String(change.assignment_state ?? "pending").trim() || "pending"
    const key = `${change.channel_id}:${status}`
    const sourceName = upstreamGroupChangeSourceLabel(change)
    const current = groups.get(key)
    if (current) {
      current.changes.push(change)
      if (!current.sourceNames.includes(sourceName)) current.sourceNames.push(sourceName)
      continue
    }
    groups.set(key, {
      key,
      channelName: upstreamGroupChangeChannelLabel(change),
      status,
      statusLabel: upstreamGroupChangeAssignmentStateLabel(status),
      sourceNames: [sourceName],
      changes: [change],
    })
  }
  return Array.from(groups.values())
}

export function groupUpstreamGroupChangesByChannelAndSource(changes: GatewayUpstreamGroupChange[]): UpstreamGroupChangeChannelGroup[] {
  const groups = new Map<number, UpstreamGroupChangeChannelGroup & { sourceMap: Map<string, UpstreamGroupChangeSourceGroup> }>()
  for (const change of changes) {
    let channel = groups.get(change.channel_id)
    if (!channel) {
      channel = {
        key: String(change.channel_id),
        channelID: change.channel_id,
        channelName: upstreamGroupChangeChannelLabel(change),
        changes: [],
        sources: [],
        sourceMap: new Map(),
      }
      groups.set(change.channel_id, channel)
    }
    channel.changes.push(change)
    const sourceValue = upstreamGroupChangeSourceValue(change)
    let source = channel.sourceMap.get(sourceValue)
    if (!source) {
      source = {
        key: `${change.channel_id}:${sourceValue}`,
        sourceName: upstreamGroupChangeSourceLabel(change),
        changes: [],
      }
      channel.sourceMap.set(sourceValue, source)
      channel.sources.push(source)
    }
    source.changes.push(change)
  }
  return Array.from(groups.values(), (group) => ({
    key: group.key,
    channelID: group.channelID,
    channelName: group.channelName,
    changes: group.changes,
    sources: group.sources,
  }))
}

function upstreamGroupChangeNeedsAttention(change: GatewayUpstreamGroupChange) {
  return !upstreamGroupChangeIsActionable(change) || change.assignment_state === "conflict"
}

function upstreamGroupChangeIsWaiting(change: GatewayUpstreamGroupChange) {
  return upstreamGroupChangeIsActionable(change) && change.assignment_state === "awaiting_facts"
}

function upstreamGroupChangeIsReadyForManualAction(change: GatewayUpstreamGroupChange) {
  return upstreamGroupChangeIsActionable(change) && !upstreamGroupChangeIsWaiting(change) && !upstreamGroupChangeNeedsAttention(change)
}

export function upstreamGroupChangePendingFacetCounts(changes: GatewayUpstreamGroupChange[]): UpstreamGroupChangePendingFacetCounts {
  return {
    all: changes.length,
    actionable: changes.filter(upstreamGroupChangeIsReadyForManualAction).length,
    awaiting: changes.filter(upstreamGroupChangeIsWaiting).length,
    attention: changes.filter(upstreamGroupChangeNeedsAttention).length,
  }
}

export function filterUpstreamGroupChangesByPendingFacet(changes: GatewayUpstreamGroupChange[], facet: UpstreamGroupChangePendingFacet) {
  if (facet === "actionable") return changes.filter(upstreamGroupChangeIsReadyForManualAction)
  if (facet === "awaiting") return changes.filter(upstreamGroupChangeIsWaiting)
  if (facet === "attention") return changes.filter(upstreamGroupChangeNeedsAttention)
  return changes
}

export function reconcileUpstreamGroupChangeSelection(selectedIDs: number[], pendingChanges: GatewayUpstreamGroupChange[]) {
  const pendingIDs = new Set(pendingChanges.filter(upstreamGroupChangeIsActionable).map((change) => change.id))
  return selectedIDs.filter((id) => pendingIDs.has(id))
}

export function sharedUpstreamGroupChangeSuggestedTargets(changes: GatewayUpstreamGroupChange[]): number[] | null {
  if (changes.length === 0) return []
  const normalize = (change: GatewayUpstreamGroupChange) => Array.from(new Set(change.suggested_candidate_group_ids ?? [])).sort((left, right) => left - right)
  const first = normalize(changes[0])
  const signature = first.join(",")
  return changes.every((change) => normalize(change).join(",") === signature) ? first : null
}

/** Return the unique suggested target ids in their first-seen order. */
export function upstreamGroupChangeSuggestedTargetIDs(changes: GatewayUpstreamGroupChange[]): number[] {
  const ids: number[] = []
  const seen = new Set<number>()
  for (const change of changes) {
    for (const id of change.suggested_candidate_group_ids ?? []) {
      if (!seen.has(id)) {
        seen.add(id)
        ids.push(id)
      }
    }
  }
  return ids
}

export function upstreamGroupChangeSelectionState(changes: GatewayUpstreamGroupChange[], selectedIDs: ReadonlySet<number>): boolean | "indeterminate" {
  const actionableChanges = changes.filter(upstreamGroupChangeIsActionable)
  if (actionableChanges.length === 0) return false
  const selectedCount = actionableChanges.filter((change) => selectedIDs.has(change.id)).length
  if (selectedCount === 0) return false
  return selectedCount === actionableChanges.length ? true : "indeterminate"
}

export function upstreamGroupChangeGroupValue(change: GatewayUpstreamGroupChange) {
  if (change.upstream_group_id) return `group:${change.upstream_group_id}`
  return `name:${change.group_name}`
}

export function filterUpstreamGroupChanges(changes: GatewayUpstreamGroupChange[], filters: UpstreamGroupChangeFilters) {
  const search = filters.search.trim().toLowerCase()
  return changes.filter((change) => {
    if (filters.channel !== upstreamGroupChangeFilterAll && String(change.channel_id) !== filters.channel) return false
    if (filters.group !== upstreamGroupChangeFilterAll && upstreamGroupChangeGroupValue(change) !== filters.group) return false
    if (filters.source !== upstreamGroupChangeFilterAll && upstreamGroupChangeSourceValue(change) !== filters.source) return false
    if (!search) return true
    return [
      upstreamGroupChangeChannelLabel(change),
      change.group_name,
      upstreamGroupChangeSourceLabel(change),
      change.assignment_state,
      change.assignment_reason_code,
      upstreamGroupChangeReasonLabel(change),
    ].join(" ").toLowerCase().includes(search)
  })
}

export function buildUpstreamGroupChangeFilterOptions(changes: GatewayUpstreamGroupChange[]): UpstreamGroupChangeFilterOptions {
  const channels = new Map<string, string>()
  const groups = new Map<string, { name: string; channel: string; upstreamGroupID?: number }>()
  const sources = new Map<string, string>()
  for (const change of changes) {
    channels.set(String(change.channel_id), upstreamGroupChangeChannelLabel(change))
    if (change.group_name) {
      groups.set(upstreamGroupChangeGroupValue(change), {
        name: change.group_name,
        channel: upstreamGroupChangeChannelLabel(change),
        upstreamGroupID: change.upstream_group_id,
      })
    }
    sources.set(upstreamGroupChangeSourceValue(change), upstreamGroupChangeSourceLabel(change))
  }
  const groupNameCounts = new Map<string, number>()
  for (const group of groups.values()) {
    const canonicalName = group.name.trim().toLowerCase()
    groupNameCounts.set(canonicalName, (groupNameCounts.get(canonicalName) ?? 0) + 1)
  }
  const sortOptions = (items: UpstreamGroupChangeFilterOption[]) => items.sort((left, right) => left.label.localeCompare(right.label, "zh-CN"))
  return {
    channels: sortOptions(Array.from(channels, ([value, label]) => ({ value, label }))),
    groups: sortOptions(Array.from(groups, ([value, group]) => {
      const duplicateStableLabel = group.upstreamGroupID != null
        && (groupNameCounts.get(group.name.trim().toLowerCase()) ?? 0) > 1
      return {
        value,
        label: duplicateStableLabel
          ? `${group.name}（${group.channel}，分组 #${group.upstreamGroupID}）`
          : group.name,
      }
    })),
    sources: sortOptions(Array.from(sources, ([value, label]) => ({ value, label }))),
  }
}
