import assert from "node:assert/strict"
import test from "node:test"
import { channelAccountConcurrencyPresentation } from "./channel-account-concurrency"

test("只展示上游明确返回的账号级并发额度", () => {
  assert.deepEqual(channelAccountConcurrencyPresentation({
    concurrency_limit: 3,
    concurrency_scope: "account",
    concurrency_source: "sub2api.auth.me.concurrency",
    concurrency_observed_at: "2026-07-23T12:00:00Z",
  }), {
    label: "账号并发 3",
    sourceLabel: "账号信息接口",
    observedAt: "2026-07-23T12:00:00Z",
    state: "bounded",
  })
  assert.deepEqual(channelAccountConcurrencyPresentation({
    concurrency_limit: 0,
    concurrency_scope: "account",
    concurrency_source: "sub2api.auth.login.user.concurrency",
    concurrency_observed_at: "2026-07-23T12:01:00Z",
  }), {
    label: "账号并发不限",
    sourceLabel: "登录响应",
    observedAt: "2026-07-23T12:01:00Z",
    state: "unlimited",
  })
})

test("未知、错误作用域和畸形额度不伪装成并发上限", () => {
  assert.equal(channelAccountConcurrencyPresentation(), null)
  assert.equal(channelAccountConcurrencyPresentation(null), null)
  assert.equal(channelAccountConcurrencyPresentation({ concurrency_limit: 4, concurrency_scope: "key" }), null)
  assert.equal(channelAccountConcurrencyPresentation({ concurrency_limit: -1, concurrency_scope: "account" }), null)
  assert.equal(channelAccountConcurrencyPresentation({ concurrency_limit: 1.5, concurrency_scope: "account" }), null)
  assert.equal(channelAccountConcurrencyPresentation({
    concurrency_limit: 2,
    concurrency_scope: "account",
    concurrency_observed_at: "2026-07-23T12:00:00Z",
  }), null)
  assert.equal(channelAccountConcurrencyPresentation({
    concurrency_limit: 2,
    concurrency_scope: "account",
    concurrency_source: "sub2api.auth.me.concurrency",
  }), null)
  assert.equal(channelAccountConcurrencyPresentation({
    concurrency_limit: 2,
    concurrency_scope: "account",
    concurrency_source: "sub2api.auth.me.concurrency",
    concurrency_observed_at: "not-a-time",
  }), null)
})

test("未知来源使用中性文案而不暴露原始标识", () => {
  assert.equal(channelAccountConcurrencyPresentation({
    concurrency_limit: 8,
    concurrency_scope: "account",
    concurrency_source: "provider.future.field",
    concurrency_observed_at: "2026-07-23T12:02:00Z",
  })?.sourceLabel, "上游账号接口")
})
