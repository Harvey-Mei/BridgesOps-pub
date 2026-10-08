import assert from "node:assert/strict"
import test from "node:test"
import { gatewayRequestHasRealFallback, gatewayRouteAttemptLocation, gatewayRouteAttemptReasonLabel, gatewayRouteOutcome, gatewayRouteTraceCopyText, gatewaySessionAffinityRemaining } from "./gateway-route-attempts"

test("derives route outcomes without treating redirects or dirty success rows as selected", () => {
  assert.equal(gatewayRouteOutcome({ statusCode: 200 }), "selected")
  assert.equal(gatewayRouteOutcome({ statusCode: 200, hasError: true }), "failed")
  assert.equal(gatewayRouteOutcome({ statusCode: 200, routeReason: "all_failed" }), "failed")
  assert.equal(gatewayRouteOutcome({ statusCode: 302 }), "failed")
  assert.equal(gatewayRouteOutcome({ statusCode: 499, canceled: true }), "canceled")
})

test("requires frozen physical evidence before showing a successful fallback", () => {
  assert.equal(gatewayRequestHasRealFallback({ route_reason: "fallback", real_fallback: true }), true)
  assert.equal(gatewayRequestHasRealFallback({ route_reason: "fallback", real_fallback: false }), false)
  assert.equal(gatewayRequestHasRealFallback({ route_reason: "smart_primary", real_fallback: true }), false)
})

test("formats fallback attempts in their recorded order without raw route text", () => {
  const copy = gatewayRouteTraceCopyText([
    { channel_id: 7, channel_name: "首选渠道", group_name: "标准", reason: "stream_account_limit", summary: "workspace concurrency limit reached", status_code: 429, eligible_rank: 1, physically_dispatched: true, first_byte_budget_ms: 30_000 },
    { channel_id: 8, channel_name: "备用渠道", group_name: "高级", reason: "transport", eligible_rank: 2, physically_dispatched: true, first_byte_budget_ms: 90_000 },
  ], { channelName: "最终渠道", groupName: "稳定", eligibleRank: 3 })

  assert.equal(copy, [
    "路由轨迹",
    "第 1 次尝试：首选渠道 / 标准 · 原因：上游容量、并发或限流 · HTTP：429 · 正常候选排名：#1 · 上游尝试：已发起 · 首字预算：30 秒",
    "  具体故障：workspace concurrency limit reached",
    "第 2 次尝试：备用渠道 / 高级 · 原因：连接上游失败 · 正常候选排名：#2 · 上游尝试：已发起 · 首字预算：90 秒",
    "最终命中：最终渠道 / 稳定 · 正常候选排名：#3",
  ].join("\n"))
  assert.equal(copy.includes("route_details"), false)
})

test("uses safe fallback labels for malformed or unknown attempts", () => {
  assert.equal(gatewayRouteAttemptReasonLabel("untrusted raw error"), "上游请求失败")
  assert.equal(gatewayRouteAttemptLocation({ channel_id: 9, channel_name: "", reason: "unknown", physically_dispatched: false, first_byte_budget_ms: 0 }), "未命名渠道")
  assert.match(
    gatewayRouteTraceCopyText([], { channelID: 9 }),
    /未保存结构化失败轨迹（历史记录不会显示原始错误）。/,
  )
})

test("labels account-concurrency admission separately from provider failures", () => {
  assert.equal(gatewayRouteAttemptReasonLabel("upstream_account_concurrency"), "上游账户并发准入受限")
  assert.equal(gatewayRouteAttemptReasonLabel("response_body_started"), "上游响应已开始但未完整结束")
  assert.equal(gatewayRouteAttemptReasonLabel("response_spool_local"), "上游尝试后本地响应缓冲不可用")
})

test("formats all-failed and session-affinity outcomes without inventing a selected route", () => {
  assert.equal(gatewayRouteTraceCopyText([
    { channel_id: 7, channel_name: "首选渠道", group_name: "标准", reason: "response_timeout", eligible_rank: 1, physically_dispatched: false, first_byte_budget_ms: 0 },
  ], { outcome: "failed" }), [
    "路由轨迹",
    "第 1 次尝试：首选渠道 / 标准 · 原因：首个有效响应超时 · 正常候选排名：#1",
    "结果：未命中可用候选",
  ].join("\n"))

  assert.equal(gatewayRouteTraceCopyText([], {
    channelName: "亲和渠道",
    groupName: "稳定",
    eligibleRank: 4,
    affinityOriginRequestID: 123,
    includeMissingAttemptsNotice: false,
  }), [
    "路由轨迹",
    "会话亲和来源：请求 #123",
    "最终命中：亲和渠道 / 稳定 · 正常候选排名：#4",
  ].join("\n"))

  assert.match(
    gatewayRouteTraceCopyText([], { outcome: "canceled" }),
    /结果：客户端取消$/,
  )
})

test("copies frozen session-affinity expiry and displaced normal candidates", () => {
  const copy = gatewayRouteTraceCopyText([], {
    channelName: "亲和渠道",
    groupName: "稳定",
    affinityOriginRequestID: 123,
    affinityExpiresAt: "2026-08-19T12:34:56Z",
    affinityOverriddenCandidates: [{ channel_id: 7, channel_name: "首选渠道", group_name: "标准", eligible_rank: 1 }],
    includeMissingAttemptsNotice: false,
  })
  assert.match(copy, /会话亲和到期：2026-08-19T12:34:56Z/)
  assert.match(copy, /被亲和覆盖：正常排名 #1 · 首选渠道 \/ 标准/)
})

test("formats the live session-affinity countdown", () => {
  const now = Date.parse("2026-08-19T12:00:00Z")
  assert.equal(gatewaySessionAffinityRemaining("2026-08-19T13:02:03Z", now), "剩余 1 小时 2 分")
  assert.equal(gatewaySessionAffinityRemaining("2026-08-19T12:02:03Z", now), "剩余 2 分 3 秒")
  assert.equal(gatewaySessionAffinityRemaining("2026-08-19T11:59:59Z", now), "已失效")
  assert.equal(gatewaySessionAffinityRemaining("invalid", now), "")
})
