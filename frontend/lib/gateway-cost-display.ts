import type { GatewayCostStatus } from "./api-types"

type GatewayCostDisplayRecord = {
  cost_status: GatewayCostStatus | string
  cost: number
  cost_source?: string | null
  cost_unavailable_reason?: string | null
  price_fact_id?: number | null
  price_fact_schema_version?: number | null
  calculated_cost?: number | null
  calculated_cost_status?: string | null
  calculated_cost_unavailable_reason?: string | null
  upstream_cost?: number | null
  upstream_cost_status?: string | null
  display_cost?: number | null
  cost_difference?: number | null
  cost_difference_ratio?: number | null
  cost_difference_material?: boolean
  group_ratio?: number | null
  completion_ratio?: number | null
  raw_group_ratio?: number | null
  raw_completion_ratio?: number | null
  funding_factor?: number | null
  prompt_tokens?: number | null
  completion_tokens?: number | null
  cache_creation_tokens?: number | null
  cache_read_tokens?: number | null
  total_tokens?: number | null
}

const gatewayCostStatusesWithAmount = new Set(["estimated", "reported", "reconciled"])
const gatewayDefaultQuotaPerUSD = 500_000
const gatewayFallbackCacheInputPriceMultiplier = 0.1

export function gatewayCostHasDisplayAmount(status: GatewayCostStatus | string | null | undefined) {
  return gatewayCostStatusesWithAmount.has(String(status ?? "").trim().toLowerCase())
}

type GatewayCostDisplayFields = Pick<
  GatewayCostDisplayRecord,
  "cost_status" | "cost" | "cost_source" | "price_fact_id" | "display_cost" | "upstream_cost" | "upstream_cost_status" | "calculated_cost" | "calculated_cost_status" | "calculated_cost_unavailable_reason" |
  "group_ratio" | "completion_ratio" | "prompt_tokens" | "completion_tokens" | "cache_creation_tokens" | "cache_read_tokens" | "total_tokens"
>

type GatewayCostRatioFields = Pick<
  GatewayCostDisplayRecord,
  "group_ratio" | "completion_ratio" | "raw_group_ratio" | "raw_completion_ratio" | "prompt_tokens" | "completion_tokens" | "cache_creation_tokens" | "cache_read_tokens" | "total_tokens"
>

export type GatewayCostRatioEstimate = {
  input: number
  output: number
  total: number
}

export type GatewayCostComparison = {
  systemEstimate: number
  upstreamCost: number
  difference: number
  ratio: number | null
  material: boolean
}

const gatewayCostDifferenceMinimumRatio = 0.05

export function gatewayFrozenRatios(record: Pick<GatewayCostDisplayRecord, "group_ratio" | "completion_ratio" | "raw_group_ratio" | "raw_completion_ratio" | "price_fact_id" | "price_fact_schema_version">) {
  const explicitFactRatios = record.price_fact_schema_version === 2 && record.price_fact_id != null && record.price_fact_id > 0
  const input = record.group_ratio
  if (input == null || !Number.isFinite(input) || input < 0 || (input === 0 && record.raw_group_ratio == null && !(record.price_fact_id && record.price_fact_id > 0))) return null
  // Legacy zero completion ratios mean same as input. Raw zero is persisted
  // for those legacy defaults too; only v2 facts freeze explicit multipliers.
  const output = record.completion_ratio
  const outputKnown = output != null && Number.isFinite(output) && output >= 0 && (output > 0 || explicitFactRatios)
  return { input, output: outputKnown ? output : input }
}

export function gatewayCostRatioEstimate(record: GatewayCostRatioFields): GatewayCostRatioEstimate | null {
  const ratios = gatewayFrozenRatios(record)
  if (ratios === null) return null
  const inputRatio = ratios.input
  const outputRatio = ratios.output
  const promptTokens = finiteNonNegative(record.prompt_tokens)
  const cacheCreationTokens = finiteNonNegative(record.cache_creation_tokens)
  const cacheReadTokens = finiteNonNegative(record.cache_read_tokens)
  const completionTokens = finiteNonNegative(record.completion_tokens)
  // This fallback has no independent cache prices. Apply the industry-default
  // 0.1x cache multiplier so cache-heavy requests are neither free nor charged
  // at the full input price. Price-fact responses use independent cache unit
  // prices instead.
  const totalTokens = finiteNonNegative(record.total_tokens)
  const uncachedInputTokens = Math.max(0, promptTokens - cacheCreationTokens - cacheReadTokens)
  const cacheTokens = cacheCreationTokens + cacheReadTokens
  const inputTokenBasis = promptTokens === 0 && completionTokens === 0 && cacheCreationTokens === 0 && cacheReadTokens === 0
    ? totalTokens
    : uncachedInputTokens + cacheTokens * gatewayFallbackCacheInputPriceMultiplier
  const input = (inputTokenBasis * inputRatio) / gatewayDefaultQuotaPerUSD
  const output = (completionTokens * outputRatio) / gatewayDefaultQuotaPerUSD
  const total = input + output
  if (!Number.isFinite(total) || total <= 0) return null
  return { input, output, total }
}

export function gatewayCostDisplayAmount(record: GatewayCostDisplayFields): number | null {
  // New API responses keep this precedence explicit, including a valid zero.
  // If any new amount field is present, do not fall back to the legacy audit
  // `cost`, which can be an old ratio-based estimate.
  if (hasPriceComparisonFields(record)) {
    for (const value of [record.upstream_cost, record.calculated_cost, record.display_cost]) {
      if (isFiniteAmount(value)) return value
    }
  }
  const confirmed = legacyConfirmedCost(record)
  if (confirmed !== null) return confirmed
  const ratioEstimate = gatewayCostUsesPriceFact(record) ? null : gatewayCostRatioEstimate(record)
  if (ratioEstimate !== null) return ratioEstimate.total
  if (hasPriceComparisonFields(record)) return null
  const status = String(record.cost_status ?? "").trim().toLowerCase()
  if (!gatewayCostHasDisplayAmount(status)) return null
  return isFiniteAmount(record.cost) ? record.cost : 0
}

export function gatewayCostDisplayText(record: GatewayCostDisplayFields, formatCost: (value: number) => string) {
  const amount = gatewayCostDisplayAmount(record)
  if (amount !== null) return formatCost(amount)
  if (String(record.cost_status ?? "").trim().toLowerCase() === "unavailable") return "无法估算"
  return "—"
}

export function gatewayCostDisplaySource(record: GatewayCostDisplayFields) {
  if (isFiniteAmount(record.upstream_cost)) return "upstream" as const
  if (isFiniteAmount(record.calculated_cost)) return "calculated" as const
  if (isFiniteAmount(record.display_cost)) return "display" as const
  if (legacyConfirmedCost(record) !== null) return "legacy" as const
  if (!gatewayCostUsesPriceFact(record) && gatewayCostRatioEstimate(record) !== null) return "ratio_estimate" as const
  if (!hasPriceComparisonFields(record) && gatewayCostHasDisplayAmount(record.cost_status)) return "legacy" as const
  return "none" as const
}

function legacyConfirmedCost(record: GatewayCostDisplayFields): number | null {
  if (hasPriceComparisonFields(record)) return null
  const status = String(record.cost_status ?? "").trim().toLowerCase()
  return (status === "reported" || status === "reconciled") && isFiniteAmount(record.cost) ? record.cost : null
}

export function gatewayCostSystemEstimate(record: GatewayCostRatioFields & Pick<GatewayCostDisplayRecord, "price_fact_id" | "cost_source" | "calculated_cost" | "upstream_cost">): number | null {
  if (isFiniteAmount(record.calculated_cost)) return record.calculated_cost
  if (gatewayCostUsesPriceFact(record)) return null
  return gatewayCostRatioEstimate(record)?.total ?? null
}

export function gatewayCostComparison(record: GatewayCostRatioFields & Pick<GatewayCostDisplayRecord, "price_fact_id" | "cost_source" | "calculated_cost" | "upstream_cost">): GatewayCostComparison | null {
  const systemEstimate = gatewayCostSystemEstimate(record)
  if (systemEstimate === null || systemEstimate < 0 || !isFiniteAmount(record.upstream_cost)) return null
  const difference = record.upstream_cost - systemEstimate
  const ratio = systemEstimate === 0 ? (difference === 0 ? 0 : null) : Math.abs(difference) / systemEstimate
  return {
    systemEstimate,
    upstreamCost: record.upstream_cost,
    difference,
    ratio,
    material: gatewayCostDifferenceExceedsThreshold(systemEstimate, record.upstream_cost),
  }
}

function gatewayCostDifferenceExceedsThreshold(systemEstimate: number, upstreamCost: number) {
  const difference = Math.abs(upstreamCost - systemEstimate)
  if (systemEstimate === 0) return difference !== 0
  const limit = systemEstimate * gatewayCostDifferenceMinimumRatio
  const tolerance = 32 * Number.EPSILON * Math.max(difference, limit)
  return difference > limit + tolerance
}

export function gatewayCostComparisonLabel(source: ReturnType<typeof gatewayCostDisplaySource>) {
  switch (source) {
    case "upstream":
      return "上游"
    case "calculated":
      return "系统估计"
    case "display":
      return "账单"
    case "ratio_estimate":
      return "倍率估算"
    case "legacy":
      return "历史"
    default:
      return "无"
  }
}

export function gatewayCostHasComparison(record: GatewayCostRatioFields & Pick<GatewayCostDisplayRecord, "price_fact_id" | "cost_source" | "calculated_cost" | "upstream_cost">) {
  return gatewayCostComparison(record) !== null
}

export function gatewayCostDifferenceText(record: Pick<GatewayCostDisplayRecord, "cost_difference">, formatCost: (value: number) => string) {
  const value = record.cost_difference
  if (!isFiniteAmount(value)) return "—"
  const sign = value > 0 ? "+" : value < 0 ? "-" : ""
  return `${sign}${formatCost(Math.abs(value))}`
}

export function gatewayCostDifferenceRatioText(record: Pick<GatewayCostDisplayRecord, "cost_difference_ratio">) {
  const ratio = record.cost_difference_ratio
  if (!isFiniteAmount(ratio)) return "—"
  return `${(ratio * 100).toLocaleString("en-US", { maximumFractionDigits: 2 })}%`
}

function isFiniteAmount(value: number | null | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value)
}



function finiteNonNegative(value: number | null | undefined) {
  return isFiniteAmount(value) ? Math.max(0, value) : 0
}

function hasPriceComparisonFields(record: Pick<GatewayCostDisplayRecord, "cost_source" | "price_fact_id" | "display_cost" | "upstream_cost" | "calculated_cost">) {
  return gatewayCostUsesPriceFact(record) ||
    "display_cost" in record ||
    "upstream_cost" in record ||
    "calculated_cost" in record ||
    ("price_fact_id" in record && record.price_fact_id != null)
}

export function gatewayCostStatusText(status: GatewayCostStatus | string | null | undefined) {
  switch (String(status ?? "").trim().toLowerCase()) {
    case "estimated":
      return "本地估算"
    case "unavailable":
      return "无法估算"
    case "reported":
      return "上游已返回"
    case "reconciled":
      return "已核对"
    case "none":
      return "无费用"
    default:
      return "费用状态未知"
  }
}

export function gatewayCostUnavailableReasonText(value: string | null | undefined) {
  const labels: Record<string, string> = {
    scope_incomplete: "计价范围信息不完整",
    operation_ambiguous: "无法确定请求计费操作",
    operation_unsupported: "该请求类型尚无计价规则",
    identity_changed: "派发身份已变化，无法匹配价格事实",
    fact_missing: "未找到匹配的价格事实",
    fact_expired: "匹配的价格事实已过期",
    fact_not_yet_valid: "价格事实尚未生效",
    schema_unsupported: "价格或用量版本不受支持",
    checksum_mismatch: "价格事实校验失败",
    usage_incomplete: "上游用量分量不完整",
    usage_invalid: "上游用量数据无效",
    price_missing: "实际用量对应单价缺失",
    currency_unsupported: "价格事实币种暂不支持",
    legacy_unverified_estimate: "历史本地估算未经过上游验证",
    unknown: "缺少可验证的计价依据",
  }
  return labels[String(value ?? "").trim().toLowerCase()] ?? labels.unknown
}

export function gatewayCostUsesPriceFact(record: Pick<GatewayCostDisplayRecord, "price_fact_id" | "cost_source">) {
  return Number(record.price_fact_id ?? 0) > 0 || String(record.cost_source ?? "").trim().toLowerCase() === "price_fact"
}

export function gatewayCostShowsLegacyEstimateDetails(record: Pick<GatewayCostDisplayRecord, "cost_status" | "price_fact_id" | "cost_source">) {
  return gatewayCostHasDisplayAmount(record.cost_status) && !gatewayCostUsesPriceFact(record)
}
