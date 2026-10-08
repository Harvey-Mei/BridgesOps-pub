import type {
  Channel,
  ChannelCostUnitRebaseReadiness,
  ChannelCostBasisOverrideReadiness,
  ChannelCostState,
  RechargeMultiplierMode,
} from "@/lib/api-types"

export function manualFundingDraft(state: Pick<ChannelCostState, "raw_balance" | "funding_factor"> | null, paid: string, credited: string, includesCredit: boolean, historical: boolean) {
  const paidAmount = Number(paid), creditedAmount = Number(credited)
  if (!state || !paid.trim() || !credited.trim()) return { valid: false, error: "", preview: null }
  if (!Number.isFinite(paidAmount) || paidAmount < 0 || !Number.isFinite(creditedAmount) || creditedAmount <= 0) return { valid: false, error: "实际付款不能为负，上游到账必须大于 0", preview: null }
  // Historical funding uses server-side snapshots, never today's balance.
  if (historical) return { valid: true, error: "", preview: null }
  const beforeRaw = includesCredit ? state.raw_balance - creditedAmount : state.raw_balance
  if (beforeRaw < 0) return { valid: false, error: "到账金额超过当前余额；如已消费，请在补充信息中填写实际充值时间", preview: null }
  const afterRaw = beforeRaw + creditedAmount
  const afterActual = beforeRaw * state.funding_factor + paidAmount
  const factor = afterActual / afterRaw
  if (![beforeRaw, afterRaw, afterActual, factor].every(Number.isFinite)) return { valid: false, error: "金额或当前成本状态无效，请核对后重试", preview: null }
  return { valid: true, error: "", preview: { beforeRaw, afterRaw, afterActual, factor } }
}

export interface ChannelCostUnitRebaseIntent {
  expected_version: number
  previous_multiplier: number
  current_multiplier: number
  note: string
}

export interface ChannelCostBasisOverrideIntent {
  expected_version: number
  expected_identity_revision: number
  expected_cost_unit_epoch: number
  funding_factor: number
  note: string
}

export function channelAccountingNeedsReview(channel: Pick<Channel, "accounting_status">) {
  return channel.accounting_status === "needs_review"
}

export function channelAccountingHasError(channel: Pick<Channel, "accounting_status">) {
  return channel.accounting_status === "error"
}

export function providerFactorForMultiplier(multiplier: number, mode: RechargeMultiplierMode) {
  if (!Number.isFinite(multiplier) || multiplier <= 0) return null
  return mode === "multiply" ? multiplier : 1 / multiplier
}

export function costUnitRebaseIntent(
  readiness: ChannelCostUnitRebaseReadiness,
  currentMultiplierText: string,
  noteText: string,
): { intent: ChannelCostUnitRebaseIntent | null; error: string | null } {
  if (!readiness.eligible || readiness.previous_multiplier == null || readiness.expected_version <= 0) {
    return { intent: null, error: "当前账务证据尚不允许执行重基线" }
  }
  const currentMultiplier = Number(currentMultiplierText)
  if (!Number.isFinite(currentMultiplier) || currentMultiplier <= 0) {
    return { intent: null, error: "请输入有效的当前上游倍率" }
  }
  if (currentMultiplier === readiness.previous_multiplier) {
    return { intent: null, error: "当前倍率必须与旧倍率不同" }
  }
  const note = noteText.trim()
  if (!note) return { intent: null, error: "请填写本次口径变更的核对说明" }
  if (new TextEncoder().encode(note).byteLength > 1024) {
    return { intent: null, error: "核对说明不能超过 1024 字节" }
  }
  return {
    intent: {
      expected_version: readiness.expected_version,
      previous_multiplier: readiness.previous_multiplier,
      current_multiplier: currentMultiplier,
      note,
    },
    error: null,
  }
}

export function channelCostUnitRebaseIdempotencyKey(
  channelID: number,
  expectedVersion: number,
  randomUUID?: () => string,
) {
  const uuid = randomUUID?.() ?? globalThis.crypto?.randomUUID?.()
  const suffix = uuid ?? `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
  return `cost-unit-rebase:${channelID}:${expectedVersion}:${suffix}`.slice(0, 128)
}

export function channelCostUnitRebaseBlockerLabel(blocker: string) {
  const labels: Record<string, string> = {
    accounting_state_unavailable: "账务状态不可用",
    provider_not_supported: "当前上游类型不支持自动重基线",
    manual_multiplier_configured: "渠道使用手工倍率配置",
    accounting_review_not_active: "账务复核状态已经变化",
    review_reason_not_rebasable: "当前复核原因不能通过自动重基线处理",
    pending_funding_candidates: "仍有待确认充值",
    accounting_fence_unavailable: "账务版本或口径代次证据不完整",
    financial_anchors_incomplete: "余额或成本基线尚未完整采集",
    previous_provider_factor_unavailable: "缺少可信的旧上游倍率",
    previous_multiplier_unavailable: "无法换算旧上游倍率",
  }
  return labels[blocker] ?? "账务证据尚未满足执行条件"
}

export function costBasisOverrideIntent(
  readiness: ChannelCostBasisOverrideReadiness,
  fundingFactorText: string,
  noteText: string,
): { intent: ChannelCostBasisOverrideIntent | null; error: string | null } {
  if (!readiness.eligible || readiness.expected_version <= 0 || readiness.expected_identity_revision <= 0 || readiness.expected_cost_unit_epoch <= 0) {
    return { intent: null, error: "当前账务状态尚不允许手动覆盖" }
  }
  const normalizedFundingFactorText = fundingFactorText.trim()
  if (!normalizedFundingFactorText) {
    return { intent: null, error: "请输入有效的成本系数" }
  }
  const fundingFactor = Number(normalizedFundingFactorText)
  if (!Number.isFinite(fundingFactor) || fundingFactor < 0) {
    return { intent: null, error: "请输入有效的成本系数" }
  }
  const note = noteText.trim()
  if (!note) return { intent: null, error: "请填写本次手动覆盖的原因" }
  if (new TextEncoder().encode(note).byteLength > 1024) {
    return { intent: null, error: "覆盖原因不能超过 1024 字节" }
  }
  return {
    intent: {
      expected_version: readiness.expected_version,
      expected_identity_revision: readiness.expected_identity_revision,
      expected_cost_unit_epoch: readiness.expected_cost_unit_epoch,
      funding_factor: fundingFactor,
      note,
    },
    error: null,
  }
}

export function channelCostBasisOverrideIdempotencyKey(
  channelID: number,
  expectedVersion: number,
  randomUUID?: () => string,
) {
  const uuid = randomUUID?.() ?? globalThis.crypto?.randomUUID?.()
  const suffix = uuid ?? `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
  return `cost-basis-override:${channelID}:${expectedVersion}:${suffix}`.slice(0, 128)
}

export function channelCostBasisOverrideBlockerLabel(blocker: string) {
  const labels: Record<string, string> = {
    accounting_state_unavailable: "账务状态不可用",
    channel_deletion_pending: "渠道正在删除",
    pending_funding_candidates: "仍有待确认充值",
    accounting_fence_unavailable: "账务版本或渠道代次证据不完整",
    current_funding_factor_invalid: "当前成本系数无效",
  }
  return labels[blocker] ?? "当前账务状态不允许覆盖"
}
