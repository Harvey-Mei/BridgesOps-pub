import type { GatewayRequestLog, GatewayRouteAttempt, GatewaySessionAffinityCandidate } from "@/lib/api-types"

export type GatewayRouteTraceOutcome = "selected" | "failed" | "canceled"

export function gatewayRequestHasRealFallback(log: Pick<GatewayRequestLog, "real_fallback" | "route_reason">) {
  return log.real_fallback === true && String(log.route_reason ?? "").trim() === "fallback"
}

export function gatewayRouteOutcome({
  statusCode,
  routeReason,
  hasError = false,
  canceled = false,
}: {
  statusCode: number
  routeReason?: string | null
  hasError?: boolean
  canceled?: boolean
}): GatewayRouteTraceOutcome {
  if (canceled) return "canceled"
  if (String(routeReason ?? "").trim() === "all_failed") return "failed"
  return statusCode >= 200 && statusCode < 300 && !hasError ? "selected" : "failed"
}

export function gatewayRouteAttemptReasonLabel(reason: string | null | undefined) {
  switch (String(reason ?? "").trim()) {
    case "candidate_resolution":
      return "未找到可用候选"
    case "upstream_key":
      return "上游密钥不可用"
    case "target_resolution":
      return "上游目标不可用"
    case "model_family":
      return "模型家族不匹配"
    case "transport":
      return "连接上游失败"
    case "model_unsupported":
      return "请求模型不受支持"
    case "context_limit":
      return "请求超过上游模型上下文容量"
    case "response_read":
      return "读取上游响应失败"
    case "response_body_started":
      return "上游响应已开始但未完整结束"
    case "response_spool_local":
      return "上游尝试后本地响应缓冲不可用"
    case "response_timeout":
      return "首个有效响应超时"
    case "stream_prelude":
      return "上游流无法启动"
    case "stream_error":
      return "首个有效输出前流失败"
    case "stream_empty":
      return "上游流未提供有效输出"
    case "stream_account_limit":
      return "上游容量、并发或限流"
    case "upstream_account_unavailable":
      return "上游账户或工作区不可用"
    case "upstream_account_concurrency":
      return "上游账户并发准入受限"
    case "upstream_key_capacity":
      return "上游密钥容量、并发或限流"
    case "mapping_runtime_fallback":
      return "映射后的模型被拒绝"
    case "upstream_status":
    case "unknown":
    default:
      return "上游请求失败"
  }
}

export function gatewayRouteAttemptLocation(attempt: GatewayRouteAttempt) {
  const channel = String(attempt.channel_name ?? "").trim() || "未命名渠道"
  const group = String(attempt.group_name ?? "").trim()
  return group ? `${channel} / ${group}` : channel
}

export function gatewayRouteAttemptBudgetLabel(value: number | null | undefined) {
  const milliseconds = Number(value)
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) return ""
  if (milliseconds < 1000) return `${Math.round(milliseconds)} ms`
  const seconds = milliseconds / 1000
  return `${Number.isInteger(seconds) ? seconds.toFixed(0) : seconds.toFixed(1)} 秒`
}

export function gatewaySessionAffinityRemaining(expiresAt: string | null | undefined, now: number = Date.now()) {
  let seconds = Math.floor((Date.parse(String(expiresAt ?? "").trim()) - now) / 1000)
  if (!Number.isFinite(seconds)) return ""
  if (seconds <= 0) return "已失效"
  const hours = Math.floor(seconds / 3600)
  seconds -= hours * 3600
  const minutes = Math.floor(seconds / 60)
  seconds -= minutes * 60
  if (hours > 0) return `剩余 ${hours} 小时 ${minutes} 分`
  if (minutes > 0) return `剩余 ${minutes} 分 ${seconds} 秒`
  return `剩余 ${seconds} 秒`
}

export function gatewayRouteTraceCopyText(
  attempts: GatewayRouteAttempt[] | null | undefined,
  selected: {
    channelName?: string | null
    channelID?: number
    groupName?: string | null
    eligibleRank?: number
    outcome?: GatewayRouteTraceOutcome
    affinityOriginRequestID?: number
    affinityExpiresAt?: string | null
    affinityOverriddenCandidates?: GatewaySessionAffinityCandidate[] | null
    includeMissingAttemptsNotice?: boolean
  },
) {
  const lines = ["路由轨迹"]
  if (Number(selected.affinityOriginRequestID) > 0) {
    lines.push(`会话亲和来源：请求 #${selected.affinityOriginRequestID}`)
    const expiresAt = String(selected.affinityExpiresAt ?? "").trim()
    if (expiresAt) lines.push(`会话亲和到期：${expiresAt}`)
    for (const candidate of Array.isArray(selected.affinityOverriddenCandidates) ? selected.affinityOverriddenCandidates : []) {
      const channel = String(candidate.channel_name ?? "").trim() || "未命名渠道"
      const group = String(candidate.group_name ?? "").trim()
      const rank = Number(candidate.eligible_rank)
      lines.push(`被亲和覆盖：${rank > 0 ? `正常排名 #${rank} · ` : ""}${group ? `${channel} / ${group}` : channel}`)
    }
  }
  const safeAttempts = Array.isArray(attempts) ? attempts : []
  if (safeAttempts.length === 0 && selected.includeMissingAttemptsNotice !== false) {
    lines.push("未保存结构化失败轨迹（历史记录不会显示原始错误）。")
  } else {
    for (const [index, attempt] of safeAttempts.entries()) {
      const parts = [`第 ${index + 1} 次尝试：${gatewayRouteAttemptLocation(attempt)}`, `原因：${gatewayRouteAttemptReasonLabel(attempt.reason)}`]
      if (Number(attempt.status_code) > 0) parts.push(`HTTP：${attempt.status_code}`)
      if (Number(attempt.eligible_rank) > 0) parts.push(`正常候选排名：#${attempt.eligible_rank}`)
      if (attempt.physically_dispatched === true) parts.push("上游尝试：已发起")
      const budget = gatewayRouteAttemptBudgetLabel(attempt.first_byte_budget_ms)
      if (budget) parts.push(`首字预算：${budget}`)
      lines.push(parts.join(" · "))
      const summary = String(attempt.summary ?? "").trim()
      if (summary) lines.push(`  具体故障：${summary}`)
    }
  }

  if (selected.outcome === "failed") {
    lines.push("结果：未命中可用候选")
    return lines.join("\n")
  }
  if (selected.outcome === "canceled") {
    lines.push("结果：客户端取消")
    return lines.join("\n")
  }

  const selectedChannel = String(selected.channelName ?? "").trim() || "未命名渠道"
  const selectedGroup = String(selected.groupName ?? "").trim()
  const selectedParts = [`最终命中：${selectedGroup ? `${selectedChannel} / ${selectedGroup}` : selectedChannel}`]
  if (Number(selected.eligibleRank) > 0) selectedParts.push(`正常候选排名：#${selected.eligibleRank}`)
  lines.push(selectedParts.join(" · "))
  return lines.join("\n")
}
