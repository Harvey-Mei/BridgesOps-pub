export type GatewayRoutingEnableCandidate = {
  cooling_down?: boolean
  upstream_group_id?: number
  presence_generation?: number
  channel_identity_revision?: number
  upstream_key_configured?: boolean
  upstream_key_enabled?: boolean
  upstream_key_provisioning?: boolean
  upstream_key_available?: boolean
  channel_id?: number
  channel_name?: string | null
  group_name?: string | null
}

export function gatewayRoutingEnableEligible(candidate: GatewayRoutingEnableCandidate) {
  return candidate.cooling_down === true
    && Number.isSafeInteger(candidate.upstream_group_id)
    && Number(candidate.upstream_group_id) > 0
    && Number.isSafeInteger(candidate.presence_generation)
    && Number(candidate.presence_generation) > 0
    && Number.isSafeInteger(candidate.channel_identity_revision)
    && Number(candidate.channel_identity_revision) > 0
    && candidate.upstream_key_available === true
}

export function gatewayRoutingEnableBlockedReason(candidate: GatewayRoutingEnableCandidate) {
  if (!Number.isSafeInteger(candidate.upstream_group_id) || Number(candidate.upstream_group_id) <= 0) {
    return "缺少稳定上游分组 ID"
  }
  if (!Number.isSafeInteger(candidate.presence_generation) || Number(candidate.presence_generation) <= 0) {
    return "缺少当前分组代次"
  }
  if (!Number.isSafeInteger(candidate.channel_identity_revision) || Number(candidate.channel_identity_revision) <= 0) {
    return "缺少当前渠道账号身份"
  }
  if (candidate.upstream_key_provisioning === true) return "本地上游 Key 仍在创建"
  if (candidate.upstream_key_configured === false) return "本地上游 Key 缺失"
  if (candidate.upstream_key_enabled === false) return "本地上游 Key 已停用"
  if (candidate.upstream_key_available === false) return "本地上游 Key 不可用"
  if (candidate.upstream_key_available !== true) return "本地上游 Key 状态未加载"
  return "当前分组没有路由健康阻断"
}

export function gatewayRoutingEnableSuccessMessage(result: { changed: boolean; idempotent_replay: boolean }, groupLabel: string) {
  if (result.idempotent_replay) return `已确认此前对“${groupLabel}”的一键启用操作（幂等重放）`
  return result.changed ? `已启用“${groupLabel}”路由` : "该分组已经处于启用状态"
}

export function gatewayRoutingEnableIdempotencyKey(
  upstreamGroupID: number,
  presenceGeneration: number,
  channelIdentityRevision: number,
  randomUUID?: () => string,
) {
  if (!Number.isSafeInteger(upstreamGroupID) || upstreamGroupID <= 0) throw new Error("upstream group id is invalid")
  if (!Number.isSafeInteger(presenceGeneration) || presenceGeneration <= 0) throw new Error("presence generation is invalid")
  if (!Number.isSafeInteger(channelIdentityRevision) || channelIdentityRevision <= 0) throw new Error("channel identity revision is invalid")
  const uuid = randomUUID?.() ?? globalThis.crypto?.randomUUID?.() ?? `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
  const safeUUID = String(uuid).trim().replace(/[^A-Za-z0-9._:-]/g, "-") || "action"
  return `routing-enable:${upstreamGroupID}:${channelIdentityRevision}:${presenceGeneration}:${safeUUID}`.slice(0, 128)
}
