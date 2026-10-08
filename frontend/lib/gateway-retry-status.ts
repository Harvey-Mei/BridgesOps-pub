import type { GatewayRequestLog } from "@/lib/api-types"

export type GatewayRetryStatus = {
  label: string
  detail: string
  attempts: number
  prompted: boolean
  recovered: boolean
}

const retryPromptTerminalCodes = new Set([
  "transient_after_acceptance",
  "retry_prompt_after_exhaustion",
])

export function gatewayRetryStatus(
  log: Pick<GatewayRequestLog, "terminal_failure_code" | "prior_physically_dispatched_failure_count" | "status_code" | "error">,
): GatewayRetryStatus | null {
  const attempts = Math.max(0, Math.trunc(Number(log.prior_physically_dispatched_failure_count) || 0))
  const prompted = retryPromptTerminalCodes.has(String(log.terminal_failure_code ?? "").trim().toLowerCase())
  const recovered = log.status_code >= 200 && log.status_code < 400 && !String(log.error ?? "").trim()
  if (!prompted && attempts === 0) return null
  if (prompted) {
    return {
      label: "提示重试",
      detail: "已拦截上游故障，并向客户端发送安全重试提示",
      attempts,
      prompted: true,
      recovered: false,
    }
  }
  return {
    label: recovered ? `自动重试 ${attempts} 次` : `已重试 ${attempts} 次`,
    detail: recovered ? `上游恢复前已自动重试 ${attempts} 次` : `已自动重试 ${attempts} 次，最终请求仍失败`,
    attempts,
    prompted: false,
    recovered,
  }
}
