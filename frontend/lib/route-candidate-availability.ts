import type { GatewayManualMappedTargetGate, GatewayManualMappedTargetPreview, GatewayRouteCandidate } from "@/lib/api-types"

type RouteCandidateAvailability = Partial<Pick<
  GatewayRouteCandidate,
  "availability_reasons" | "disabled_for_key" | "cooling_down" | "routing_available"
>>

type CandidateGroupMembershipCandidate = Partial<Pick<GatewayRouteCandidate, "model_mapping_families">>
type CandidateGroupListCandidate = Partial<Pick<GatewayRouteCandidate, "channel_name" | "group_name" | "gateway_app" | "model_mapping_families">>

const manualRouteStructuralBlockingReasons = new Set([
  "manual_mapped_target_blocked",
  "channel_missing",
  "upstream_key_missing",
  "upstream_key_disabled",
  "upstream_key_unavailable",
  "upstream_generation_unavailable",
  "gateway_app_mismatch",
])

const manualRouteKnownMappedModelBlockingReasons = new Set([
  "model_not_listed",
  "model_not_supported",
])

export type ManualRouteCandidateSelectionOptions = {
  // This comes exclusively from the server-owned manual-route preview. A
  // requested/recent/admin-preview model is not a stable fixed-route fact and
  // must use the default false value.
  enforceServerConfirmedMappedModel?: boolean
}

const availabilityLabels: Record<string, string> = {
  manual_mapped_target_blocked: "服务端确认固定目标不可用",
  channel_missing: "渠道已不存在",
  monitor_disabled: "渠道监控已暂停",
  channel_error: "渠道状态异常",
  low_balance: "余额低于阈值",
  no_rate_snapshot: "暂无倍率快照",
  no_routable_group: "暂无可路由分组",
  upstream_key_missing: "渠道 Key 未就绪",
  upstream_key_disabled: "渠道 Key 已停用",
  upstream_key_unavailable: "渠道 Key 不可用",
  upstream_generation_unavailable: "上游分组已变更，请刷新后重新选择",
  models_not_synced: "模型能力未同步",
  no_supported_gateway_app: "没有兼容协议",
  gateway_app_mismatch: "协议不匹配",
  model_family_mismatch: "模型族不匹配",
  model_capability_unknown: "模型能力未知",
  model_not_listed: "不支持当前模型",
  model_not_supported: "模型已确认不支持",
  routing_cooldown: "路由冷却中",
}

export function candidateAvailabilityReasons(candidate: RouteCandidateAvailability) {
  return Array.from(new Set((candidate.availability_reasons ?? [])
    .map((reason) => String(reason ?? "").trim())
    .filter((reason) => reason !== "" && reason !== "probe_disabled")))
}

export function candidateAvailabilityLabel(reason: string) {
  return availabilityLabels[reason] ?? reason
}

export function candidateSupportsMappingFamily(candidate: CandidateGroupMembershipCandidate, modelMappingFamily: string) {
  const family = modelMappingFamily.trim().toLowerCase()
  if (!family) return true
  return (candidate.model_mapping_families ?? []).some((candidateFamily) => candidateFamily.trim().toLowerCase() === family)
}

export function candidateGroupSelectionSupportsMappingFamily(
  candidates: readonly CandidateGroupMembershipCandidate[],
  modelMappingFamily: string,
) {
  const family = modelMappingFamily.trim().toLowerCase()
  if (!family) return true
  return candidates.some((candidate) => candidateSupportsMappingFamily(candidate, family))
}

export function filterCandidateGroupMembershipCandidates<T extends CandidateGroupListCandidate>(
  candidates: readonly T[],
  search: string,
) {
  const normalizedSearch = search.trim().toLowerCase()
  if (!normalizedSearch) return [...candidates]
  return candidates.filter((candidate) => [candidate.channel_name, candidate.group_name, candidate.gateway_app]
    .join(" ").toLowerCase().includes(normalizedSearch))
}

export function manualRouteCandidateBlockingReasons(
  candidate: RouteCandidateAvailability,
  options: ManualRouteCandidateSelectionOptions = {},
) {
  // A selection happens before the request model is known. `model_not_*` from
  // a requested/recent routing preview is therefore not a target fact. The
  // gateway validates an explicit mapped target against the actual request
  // model at dispatch time, where it has authoritative mapping facts.
  return candidateAvailabilityReasons(candidate).filter((reason) => (
    manualRouteStructuralBlockingReasons.has(reason)
    || (options.enforceServerConfirmedMappedModel === true && manualRouteKnownMappedModelBlockingReasons.has(reason))
  ))
}

export function manualRouteCandidateSelectable(
  candidate: RouteCandidateAvailability,
  options: ManualRouteCandidateSelectionOptions = {},
) {
  return manualRouteCandidateBlockingReasons(candidate, options).length === 0
}

export function manualRouteCandidateBlockingReasonText(
  candidate: RouteCandidateAvailability,
  options: ManualRouteCandidateSelectionOptions = {},
) {
  return manualRouteCandidateBlockingReasons(candidate, options).map(candidateAvailabilityLabel).join("、")
}

type ManualMappedTargetCandidate = RouteCandidateAvailability & Pick<GatewayRouteCandidate, "channel_id" | "group_name">

export type ManualMappedTargetSelectionState = {
  target: GatewayManualMappedTargetGate | null
  selectable: boolean
  blockingReasons: string[]
  mappingUnresolved: boolean
}

// Generic route previews may contain facts for a requested or recent model.
// Strip those model reasons before merging the server-owned exact mapped-model
// result, otherwise an unrelated browser preview could manufacture a blocker.
export function manualMappedTargetSelectionState(
  candidate: ManualMappedTargetCandidate,
  preview: GatewayManualMappedTargetPreview | null | undefined,
): ManualMappedTargetSelectionState {
  const target = manualMappedTargetForCandidate(candidate, preview)
  const genericReasons = candidateAvailabilityReasons(candidate)
    .filter((reason) => !manualRouteKnownMappedModelBlockingReasons.has(reason))
  const serverReason = target?.structural_blocked
    ? String(target.reason ?? "").trim() || "manual_mapped_target_blocked"
    : preview?.mapping_state === "resolved" && target == null
      ? "manual_mapped_target_blocked"
      : ""
  const mergedCandidate = {
    ...candidate,
    availability_reasons: serverReason ? [...genericReasons, serverReason] : genericReasons,
  }
  const options = {
    enforceServerConfirmedMappedModel: target?.manual_mapped_model_gate === true,
  }
  const blockingReasons = manualRouteCandidateBlockingReasons(mergedCandidate, options)
  return {
    target,
    selectable: blockingReasons.length === 0,
    blockingReasons,
    mappingUnresolved: preview?.mapping_state === "unresolved",
  }
}

function manualMappedTargetForCandidate(
  candidate: Pick<GatewayRouteCandidate, "channel_id" | "group_name">,
  preview: GatewayManualMappedTargetPreview | null | undefined,
) {
  const groupName = canonicalManualMappedTargetGroup(candidate.group_name)
  return preview?.targets.find((target) => (
    target.channel_id === candidate.channel_id
    && canonicalManualMappedTargetGroup(target.group_name) === groupName
  )) ?? null
}

function canonicalManualMappedTargetGroup(groupName: string | null | undefined) {
  return String(groupName ?? "").trim().toLowerCase()
}
