import type { GatewayRequestLog } from "@/lib/api-types"

export const manualRoutingCooldownMinimumFirstByteMs = 30_000

const smartRoutingCooldownEligibleRouteReasons = new Set([
  "smart_primary",
  "fallback",
  "session_affinity",
  "model_filtered",
])

export type GatewayFirstByteTone = "missing" | "fast" | "moderate" | "slow"

export function gatewayFirstByteTone(value: number | null | undefined): GatewayFirstByteTone {
  const milliseconds = Number(value ?? 0)
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) return "missing"
  if (milliseconds <= 10_000) return "fast"
  if (milliseconds <= manualRoutingCooldownMinimumFirstByteMs) return "moderate"
  return "slow"
}

export function gatewayRequestCanBeManuallyCooled(log: Pick<GatewayRequestLog, "channel_id" | "group_name" | "first_byte_ms" | "route_reason">) {
  const routeReason = log.route_reason.trim().toLowerCase()
  return log.channel_id > 0
    && log.group_name.trim() !== ""
    && log.first_byte_ms > manualRoutingCooldownMinimumFirstByteMs
    && smartRoutingCooldownEligibleRouteReasons.has(routeReason)
}

export function gatewayManualCooldownIdempotencyKey(requestLogID: number, randomUUID?: () => string) {
  const uuid = randomUUID?.() ?? globalThis.crypto?.randomUUID?.()
  if (uuid) return `request-log-${requestLogID}-${uuid}`
  return `request-log-${requestLogID}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
}
