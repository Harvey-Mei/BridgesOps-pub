import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

import {
  gatewayRoutingEnableBlockedReason,
  gatewayRoutingEnableEligible,
  gatewayRoutingEnableIdempotencyKey,
  gatewayRoutingEnableSuccessMessage,
} from "./gateway-routing-enable"

test("routing enable requires a cooling current generation with a usable local key", () => {
  assert.equal(gatewayRoutingEnableEligible({
    cooling_down: true,
    upstream_group_id: 12,
    presence_generation: 3,
    channel_identity_revision: 8,
    upstream_key_available: true,
  }), true)
  assert.equal(gatewayRoutingEnableEligible({
    cooling_down: true,
    upstream_group_id: 12,
    presence_generation: 3,
    channel_identity_revision: 8,
    upstream_key_available: false,
  }), false)
  assert.equal(gatewayRoutingEnableEligible({
    cooling_down: true,
    upstream_group_id: 12,
    presence_generation: 0,
    channel_identity_revision: 8,
    upstream_key_available: true,
  }), false)
})

test("routing enable reports the precise structural blocker", () => {
  assert.equal(gatewayRoutingEnableBlockedReason({}), "缺少稳定上游分组 ID")
  assert.equal(gatewayRoutingEnableBlockedReason({ upstream_group_id: 12 }), "缺少当前分组代次")
  assert.equal(gatewayRoutingEnableBlockedReason({ upstream_group_id: 12, presence_generation: 3 }), "缺少当前渠道账号身份")
  const identity = { upstream_group_id: 12, presence_generation: 3, channel_identity_revision: 8 }
  assert.equal(gatewayRoutingEnableBlockedReason({ ...identity, upstream_key_provisioning: true }), "本地上游 Key 仍在创建")
  assert.equal(gatewayRoutingEnableBlockedReason({ ...identity, upstream_key_configured: false }), "本地上游 Key 缺失")
  assert.equal(gatewayRoutingEnableBlockedReason({ ...identity, upstream_key_enabled: false }), "本地上游 Key 已停用")
  assert.equal(gatewayRoutingEnableBlockedReason({ ...identity, upstream_key_available: false }), "本地上游 Key 不可用")
  assert.equal(gatewayRoutingEnableBlockedReason(identity), "本地上游 Key 状态未加载")
})

test("routing enable idempotency key is generation scoped and header safe", () => {
  assert.equal(
    gatewayRoutingEnableIdempotencyKey(12, 3, 8, () => "00000000-0000-4000-8000-000000000000"),
    "routing-enable:12:8:3:00000000-0000-4000-8000-000000000000",
  )
  assert.equal(gatewayRoutingEnableIdempotencyKey(12, 3, 8, () => "unsafe key").includes(" "), false)
})

test("routing enable distinguishes changed, already enabled, and idempotent replay results", () => {
  assert.equal(gatewayRoutingEnableSuccessMessage({ changed: true, idempotent_replay: false }, "主分组"), "已启用“主分组”路由")
  assert.equal(gatewayRoutingEnableSuccessMessage({ changed: false, idempotent_replay: false }, "主分组"), "该分组已经处于启用状态")
  assert.equal(gatewayRoutingEnableSuccessMessage({ changed: true, idempotent_replay: true }, "主分组"), "已确认此前对“主分组”的一键启用操作（幂等重放）")
  const source = readFileSync(new URL("../app/gateway-page.tsx", import.meta.url), "utf8")
  assert.match(source, /toast\.success\(gatewayRoutingEnableSuccessMessage\(result, groupLabel\)\)/)
})

test("candidate management exposes one-click enable only for cooling groups", () => {
  const source = readFileSync(new URL("../app/gateway-page.tsx", import.meta.url), "utf8")
  const start = source.indexOf("function renderRouteCandidateItem")
  const end = source.indexOf("\n  return (", start)
  const renderSource = source.slice(start, end)

  assert.ok(start >= 0 && end > start)
  assert.match(renderSource, /const routingEnableAllowed = gatewayRoutingEnableEligible\(candidate\)/)
  assert.match(renderSource, /\{cooling \? \(/)
  assert.match(renderSource, /onClick=\{\(\) => \{ if \(!interactionsDisabled\) void enableRoutingGroup\(candidate\) \}\}/)
  assert.match(renderSource, /disabled=\{interactionsDisabled \|\| routingEnableSaving \|\| !routingEnableAllowed\}/)
  assert.match(renderSource, /const routingEnableBlocker = gatewayRoutingEnableBlockedReason\(candidate\)/)
  assert.match(renderSource, /无法一键启用：\$\{routingEnableBlocker\}/)
  assert.match(renderSource, /\{routingEnableSaving \? "启用中" : "一键启用"\}/)
})

test("candidate group membership picker exposes the same guarded enable action", () => {
  const source = readFileSync(new URL("../components/gateway/candidate-group-membership-picker.tsx", import.meta.url), "utf8")
  assert.match(source, /routingEnableCandidate\?\.cooling_down \? \(/)
  assert.match(source, /gatewayRoutingEnableEligible\(routingEnableCandidate\)/)
  assert.match(source, /disabled=\{routingEnableDisabled\}/)
  assert.match(source, /onClick=\{\(\) => onEnableRoutingGroup\(routingEnableCandidate\)\}/)
  assert.match(source, /tabIndex=\{routingEnableDisabled \? 0 : undefined\}/)
  assert.match(source, /role=\{routingEnableDisabled \? "button" : undefined\}/)
  assert.match(source, /aria-label=\{routingEnableDisabled \? `一键启用[^`]+\$\{routingEnableTooltip\}`/)
  assert.match(source, /<TooltipContent[^>]*>\{routingEnableTooltip\}<\/TooltipContent>/)
})
