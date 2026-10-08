import type { UpstreamAccountConcurrencyFact } from "./api-types"
import { compactCount } from "./format"

export interface ChannelAccountConcurrencyPresentation {
  label: string
  sourceLabel: string
  observedAt: string
  state: "bounded" | "unlimited"
}

const sourceLabels: Record<string, string> = {
  "sub2api.auth.me.concurrency": "账号信息接口",
  "sub2api.auth.login.user.concurrency": "登录响应",
}

export function channelAccountConcurrencyPresentation(
  fact?: UpstreamAccountConcurrencyFact | null,
): ChannelAccountConcurrencyPresentation | null {
  if (!fact || fact.concurrency_scope !== "account") return null
  const limit = fact.concurrency_limit
  if (!Number.isSafeInteger(limit) || limit < 0) return null
  const source = String(fact.concurrency_source || "").trim()
  const observedAt = String(fact.concurrency_observed_at || "").trim()
  if (!source || !observedAt || !Number.isFinite(Date.parse(observedAt))) return null
  return {
    label: limit === 0 ? "账号并发不限" : `账号并发 ${compactCount(limit)}`,
    sourceLabel: sourceLabels[source] ?? "上游账号接口",
    observedAt,
    state: limit === 0 ? "unlimited" : "bounded",
  }
}
