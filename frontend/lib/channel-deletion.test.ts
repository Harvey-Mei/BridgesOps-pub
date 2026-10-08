import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

import type { Channel } from "./api-types"
import { channelDeletionStatusLabel, isChannelDeletionPending, markChannelDeletionPending } from "./channel-deletion"

function channel(id: number): Channel {
  return {
    id,
    name: `channel-${id}`,
    type: "sub2api",
    gateway_app: "codex",
    site_url: "https://example.com",
    username: "operator",
    sort_order: 1,
    credential_mode: "password",
    login_extra_params: "",
    turnstile_enabled: false,
    ignore_announcements: false,
    subscription_enabled: false,
    proxy_enabled: false,
    balance_threshold: 0.1,
    recharge_multiplier_mode: "divide",
    monitor_enabled: true,
    latency_probe_enabled: true,
    admin_revision: 1,
    funding_factor: 1,
    funding_factor_source: "default",
    pending_funding_count: 0,
    created_at: "2026-07-27T00:00:00Z",
    updated_at: "2026-07-27T00:00:00Z",
  }
}

test("accepted deletion marks only the requested channel pending", () => {
  const first = channel(1)
  const second = channel(2)
  const next = markChannelDeletionPending([first, second], {
    channel_id: 2,
    status: "pending_remote_cleanup",
    message: "accepted",
  })

  assert.equal(next[0], first)
  assert.equal(isChannelDeletionPending(next[0]), false)
  assert.equal(next[1].deletion_pending, true)
  assert.equal(next[1].deletion_state, "pending_remote_cleanup")
  assert.equal(isChannelDeletionPending(next[1]), true)
})

test("refreshed deletion state remains pending even when the boolean is omitted", () => {
  assert.equal(isChannelDeletionPending({ deletion_state: "pending_remote_cleanup" }), true)
  assert.equal(isChannelDeletionPending({ deletion_state: "manual_review_required" }), true)
  assert.equal(isChannelDeletionPending({ deletion_state: "ready_for_local_finalize" }), true)
  assert.equal(isChannelDeletionPending({ deletion_state: "finalized" }), false)
})

test("deletion coordinator states have distinct labels", () => {
  assert.equal(channelDeletionStatusLabel("pending_remote_cleanup"), "待远端清理")
  assert.equal(channelDeletionStatusLabel("manual_review_required"), "自动清理中")
  assert.equal(channelDeletionStatusLabel("ready_for_local_finalize"), "待本地收尾")
  assert.equal(channelDeletionStatusLabel("finalized"), "删除已完成")
})

test("pending channels cannot enter bulk sync or costing and repeated delete opens review", () => {
  const source = readFileSync(new URL("../components/monitor/channel-cards.tsx", import.meta.url), "utf8")

  assert.match(source, /if \(list\.length === 0 \|\| hasDeletionPending \|\| syncRunRef\.current \|\| anySyncRunning\) return/)
  assert.match(source, /disabled=\{anySyncRunning \|\| hasDeletionPending\}/)
  assert.match(source, /<button type="button" disabled=\{deletionPending\}/)
  assert.match(source, /onClick=\{\(\) => setCosting\(c\)\}/)
  assert.match(source, /if \(deletionPending\) \{\s*setDeletionReviewChannel\(c\)\s*return/)
  assert.match(source, /deletionPending \? "查看删除复核" : "删除渠道"/)
  assert.doesNotMatch(source, /余额历史、倍率快照与登录凭据都将一并清除/)
})
