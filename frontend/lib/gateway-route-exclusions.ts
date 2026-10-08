import type {
  GatewayRequestLog,
  GatewayRouteExclusionSnapshot,
  GatewayRouteExclusionSnapshotEntry,
} from "@/lib/api-types"

const gatewayRouteExclusionSnapshotVersion = 1
const gatewayRouteExclusionSnapshotMaxEntries = 64
const gatewayRouteExclusionSnapshotMaxTotalEntries = 1_000_000

const gatewayRouteExclusionReasons = new Set([
  "upstream_generation_unavailable",
  "upstream_temporarily_unavailable",
  "model_capability_unknown",
  "model_not_listed",
  "model_not_supported",
])

export type GatewayRouteExclusionSnapshotState =
  | { kind: "not_collected"; version: 0 }
  | { kind: "unavailable"; version: number }
  | { kind: "captured"; version: 1; snapshot: GatewayRouteExclusionSnapshot }

export function gatewayRouteExclusionSnapshotState(
  log: Pick<GatewayRequestLog, "route_exclusion_snapshot" | "route_exclusion_snapshot_version">,
): GatewayRouteExclusionSnapshotState {
  const version = Number(log.route_exclusion_snapshot_version)
  if (version === 0) return { kind: "not_collected", version: 0 }
  if (!Number.isInteger(version) || version !== gatewayRouteExclusionSnapshotVersion) {
    return { kind: "unavailable", version: Number.isInteger(version) ? version : -1 }
  }

  const snapshot = log.route_exclusion_snapshot
  if (!isValidGatewayRouteExclusionSnapshot(snapshot)) {
    return { kind: "unavailable", version }
  }
  return { kind: "captured", version, snapshot }
}

export function gatewayRouteExclusionReasonLabel(reason: string | null | undefined) {
  switch (String(reason ?? "").trim()) {
    case "upstream_generation_unavailable":
      return "上游分组 generation 不可用"
    case "upstream_temporarily_unavailable":
      return "上游暂不可用（故障隔离）"
    case "model_capability_unknown":
      return "模型能力尚未确认"
    case "model_not_listed":
      return "模型不在渠道同步列表"
    case "model_not_supported":
      return "渠道不支持该模型"
    default:
      return "其他结构化排除原因"
  }
}

export function gatewayRouteExclusionLocation(entry: GatewayRouteExclusionSnapshotEntry) {
  return `渠道 #${entry.channel_id} / ${entry.group_name}`
}

export function gatewayRouteExclusionGenerationLabel(entry: GatewayRouteExclusionSnapshotEntry) {
  const upstreamGroupID = Number(entry.upstream_group_id)
  const generation = Number(entry.presence_generation)
  if (upstreamGroupID > 0 && generation > 0) {
    return `上游分组 #${upstreamGroupID} · generation ${generation}`
  }
  return "上游分组 N/A · generation N/A"
}

export function gatewayRouteExclusionCooldownLabel(
  entry: GatewayRouteExclusionSnapshotEntry,
  formatDateTime: (value: string) => string,
) {
  const failureCountValue = Number(entry.failure_count)
  const failureCount = Number.isFinite(failureCountValue) ? failureCountValue : 0
  const cooldownUntil = String(entry.cooldown_until ?? "").trim()
  if (failureCount <= 0 && !cooldownUntil) return ""
  if (!cooldownUntil) return `失败计数 ${failureCount} · 等待连续三次探测成功`
  return `失败计数 ${failureCount > 0 ? failureCount : "N/A"} · 手动冷却截止 ${formatDateTime(cooldownUntil)}`
}

export function gatewayRouteExclusionCoverageLabel(state: GatewayRouteExclusionSnapshotState) {
  if (state.kind !== "captured") return "N/A"
  const shown = state.snapshot.entries.length
  if (state.snapshot.truncated) {
    return `已保存 ${shown} / ${state.snapshot.total_entries} 条（已截断）`
  }
  return `完整覆盖 ${state.snapshot.total_entries} 条`
}

export function gatewayRouteExclusionSnapshotCopyText(
  log: Pick<GatewayRequestLog, "route_exclusion_snapshot" | "route_exclusion_snapshot_version">,
  formatDateTime: (value: string) => string = (value) => value,
) {
  const state = gatewayRouteExclusionSnapshotState(log)
  const lines = ["候选排除快照"]
  if (state.kind === "not_collected") {
    lines.push("采集状态：未采集 / N/A")
    lines.push("说明：历史记录没有不可变快照，未使用当前状态回填。")
    return lines.join("\n")
  }
  if (state.kind === "unavailable") {
    lines.push("采集状态：快照不可用 / N/A")
    lines.push(`快照版本：${state.version > 0 ? `v${state.version}` : "N/A"}`)
    lines.push("说明：未展示不完整或不受支持的快照，未使用当前状态回填。")
    return lines.join("\n")
  }

  lines.push(`采集状态：已采集（v${state.snapshot.version}）`)
  lines.push(`采集时间：${formatDateTime(state.snapshot.captured_at)}`)
  lines.push(`覆盖范围：${gatewayRouteExclusionCoverageLabel(state)}`)
  if (state.snapshot.entries.length === 0) {
    lines.push("排除项：该快照未记录结构化排除项")
    return lines.join("\n")
  }
  state.snapshot.entries.forEach((entry, index) => {
    lines.push(`排除 ${index + 1}：${gatewayRouteExclusionLocation(entry)}`)
    lines.push(`  原因：${gatewayRouteExclusionReasonLabel(entry.reason)}`)
    lines.push(`  分组代际：${gatewayRouteExclusionGenerationLabel(entry)}`)
    const cooldown = gatewayRouteExclusionCooldownLabel(entry, formatDateTime)
    if (cooldown) lines.push(`  冷却事实：${cooldown}`)
  })
  return lines.join("\n")
}

function isValidGatewayRouteExclusionSnapshot(
  snapshot: GatewayRouteExclusionSnapshot | undefined,
): snapshot is GatewayRouteExclusionSnapshot {
  if (!snapshot || snapshot.version !== gatewayRouteExclusionSnapshotVersion) return false
  const capturedAt = Date.parse(String(snapshot.captured_at ?? ""))
  if (!Number.isFinite(capturedAt)) return false
  if (!Array.isArray(snapshot.entries) || snapshot.entries.length > gatewayRouteExclusionSnapshotMaxEntries) return false
  if (!Number.isSafeInteger(snapshot.total_entries) || snapshot.total_entries < snapshot.entries.length || snapshot.total_entries > gatewayRouteExclusionSnapshotMaxTotalEntries) return false
  if (typeof snapshot.truncated !== "boolean" || snapshot.truncated !== (snapshot.total_entries > snapshot.entries.length)) return false
  return snapshot.entries.every((entry) => isValidGatewayRouteExclusionSnapshotEntry(entry))
}

function isValidGatewayRouteExclusionSnapshotEntry(
  entry: GatewayRouteExclusionSnapshotEntry,
) {
  if (!entry || !Number.isSafeInteger(entry.channel_id) || entry.channel_id <= 0) return false
  const groupName = String(entry.group_name ?? "")
  if (!groupName || groupName !== groupName.trim().toLowerCase() || [...groupName].length > 128 || hasControlCharacter(groupName)) return false
  if (!gatewayRouteExclusionReasons.has(String(entry.reason ?? ""))) return false

  const upstreamGroupID = Number(entry.upstream_group_id ?? 0)
  const generation = Number(entry.presence_generation ?? 0)
  const failureCount = Number(entry.failure_count ?? 0)
  const cooldownUntil = String(entry.cooldown_until ?? "").trim()
  if (!isNonNegativeInteger(upstreamGroupID) || !isNonNegativeInteger(generation) || !isNonNegativeInteger(failureCount, gatewayRouteExclusionSnapshotMaxTotalEntries)) return false

  if (entry.reason === "upstream_generation_unavailable") {
    return upstreamGroupID === 0 && generation === 0 && failureCount === 0 && !cooldownUntil
  }
  if (upstreamGroupID <= 0 || generation <= 0) return false
  if (failureCount === 0 && !cooldownUntil) {
    return entry.reason !== "upstream_temporarily_unavailable"
  }
  if (failureCount <= 0) return false
  return !cooldownUntil || Number.isFinite(Date.parse(cooldownUntil))
}

function isNonNegativeInteger(value: number, max = Number.MAX_SAFE_INTEGER) {
  return Number.isSafeInteger(value) && value >= 0 && value <= max
}

function hasControlCharacter(value: string) {
  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0
    if (codePoint <= 0x1f || (codePoint >= 0x7f && codePoint <= 0x9f)) return true
  }
  return false
}
