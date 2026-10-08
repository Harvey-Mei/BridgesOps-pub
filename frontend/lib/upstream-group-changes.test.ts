import assert from "node:assert/strict"
import test from "node:test"

import type { GatewayUpstreamGroupChange } from "./api-types"
import {
  buildUpstreamGroupChangeFilterOptions,
  filterUpstreamGroupChanges,
  filterUpstreamGroupChangesByPendingFacet,
  groupUpstreamGroupChangesByChannelAndSource,
  groupUpstreamGroupChangesByChannelAndStatus,
  reconcileUpstreamGroupChangeSelection,
  sharedUpstreamGroupChangeSuggestedTargets,
  upstreamGroupChangeSuggestedTargetIDs,
  upstreamGroupChangeIsActionable,
  upstreamGroupChangePendingFacetCounts,
  upstreamGroupChangeSelectionState,
  upstreamGroupChangeFilterAll,
  upstreamGroupChangeHistoryPath,
  upstreamGroupChangeManualReviewReasonLabel,
  upstreamGroupChangeReasonLabel,
  upstreamGroupChangeRequiresManualRemovalReview,
  upstreamGroupChangeView,
} from "./upstream-group-changes"
import {
  createUpstreamGroupChangeHistoryRequestGate,
  initialUpstreamGroupChangeHistoryCursorState,
  nextUpstreamGroupChangeHistoryCursorState,
  previousUpstreamGroupChangeHistoryCursorState,
} from "./upstream-group-change-history-navigation"

function change(overrides: Partial<GatewayUpstreamGroupChange> = {}): GatewayUpstreamGroupChange {
  return {
    id: 1,
    channel_id: 2,
    channel_name: "蓝色渠道",
    group_name: "限时特惠",
    kind: "added",
    status: "pending",
    upstream_key_id: 9,
    upstream_key_name: "受管 Key",
    detected_at: "2026-07-23T00:00:00Z",
    ...overrides,
  }
}

test("changes are split into pending, manual-review, and history views", () => {
  assert.equal(upstreamGroupChangeView(change()), "pending")
  assert.equal(upstreamGroupChangeView(change({ status: "ignored" })), "history")
  assert.equal(upstreamGroupChangeView(change({ status: "ignored", kind: "removed", assignment_reason_code: "manual_review_required" })), "review")
  assert.equal(upstreamGroupChangeView(change({ status: "assigned" })), "history")
  assert.equal(upstreamGroupChangeView(change({ status: "acknowledged", kind: "removed" })), "history")
  assert.equal(upstreamGroupChangeView(change({ status: "superseded" })), "history")
})

test("filters independently match channel, group, source, and free text", () => {
  const changes = [
    change(),
    change({ id: 2, channel_id: 3, channel_name: "青色渠道", group_name: "官渠", upstream_group_id: 22, upstream_key_id: 10, upstream_key_name: "Claude Key", assignment_reason_code: "no_rule_matched" }),
  ]
  assert.deepEqual(filterUpstreamGroupChanges(changes, {
    search: "没有自动归组规则",
    channel: "3",
    group: "group:22",
    source: "key:10",
  }).map((item) => item.id), [2])
  assert.deepEqual(filterUpstreamGroupChanges(changes, {
    search: "",
    channel: upstreamGroupChangeFilterAll,
    group: "name:限时特惠",
    source: upstreamGroupChangeFilterAll,
  }).map((item) => item.id), [1])
})

test("filter options deduplicate stable identities and distinguish same-name groups", () => {
  const options = buildUpstreamGroupChangeFilterOptions([
    change({ upstream_group_id: 21 }),
    change({ id: 2, upstream_group_id: 21 }),
    change({ id: 3, channel_id: 3, channel_name: "青色渠道", upstream_group_id: 22, upstream_key_id: undefined, upstream_key_name: "" }),
  ])
  assert.equal(options.channels.length, 2)
  assert.equal(options.groups.length, 2)
  assert.equal(options.groups.find((item) => item.value === "group:21")?.label, "限时特惠（蓝色渠道，分组 #21）")
  assert.equal(options.groups.find((item) => item.value === "group:22")?.label, "限时特惠（青色渠道，分组 #22）")
  assert.ok(options.sources.some((item) => item.label === "受管 Key"))
  assert.ok(options.sources.some((item) => item.label === "未标记来源"))
})

test("generation model-sync failure is translated into an actionable reason", () => {
  assert.equal(
    upstreamGroupChangeReasonLabel(change({ assignment_state: "awaiting_facts", assignment_reason_code: "models_not_synced_for_generation" })),
    "当前分组代尚未完成模型同步；仍可人工加入，自动归组会等待事实",
  )
})

test("pending changes group by channel and assignment status while retaining sources", () => {
  const groups = groupUpstreamGroupChangesByChannelAndStatus([
    change({ id: 1, assignment_state: "awaiting_facts", upstream_key_name: "Key A" }),
    change({ id: 2, assignment_state: "conflict", upstream_key_name: "Key B" }),
    change({ id: 3, assignment_state: "ready", upstream_key_name: "Key C" }),
    change({ id: 4, assignment_state: "awaiting_facts", upstream_key_name: "Key D" }),
  ])

  assert.deepEqual(groups.map((group) => [group.status, group.changes.map((item) => item.id)]), [
    ["awaiting_facts", [1, 4]],
    ["conflict", [2]],
    ["ready", [3]],
  ])
  assert.deepEqual(groups[0].sourceNames, ["Key A", "Key D"])
})

test("pending facets partition actionable, waiting, and attention states", () => {
  const changes = [
    change({ id: 1, assignment_state: "ready" }),
    change({ id: 2, assignment_state: "awaiting_facts" }),
    change({ id: 3, assignment_state: "conflict" }),
    change({ id: 4, assignment_state: "awaiting_facts", assignment_actionable: false }),
  ]

  assert.deepEqual(upstreamGroupChangePendingFacetCounts(changes), {
    all: 4,
    actionable: 1,
    awaiting: 1,
    attention: 2,
  })
  assert.deepEqual(filterUpstreamGroupChangesByPendingFacet(changes, "actionable").map((item) => item.id), [1])
  assert.deepEqual(filterUpstreamGroupChangesByPendingFacet(changes, "awaiting").map((item) => item.id), [2])
  assert.deepEqual(filterUpstreamGroupChangesByPendingFacet(changes, "attention").map((item) => item.id), [3, 4])
})

test("pending changes group by channel and then source for bulk selection", () => {
  const groups = groupUpstreamGroupChangesByChannelAndSource([
    change({ id: 1, upstream_key_id: 9, upstream_key_name: "Key A" }),
    change({ id: 2, upstream_key_id: 9, upstream_key_name: "Key A" }),
    change({ id: 3, upstream_key_id: 10, upstream_key_name: "Key B" }),
    change({ id: 4, channel_id: 3, channel_name: "青色渠道", upstream_key_id: 9, upstream_key_name: "Key A" }),
  ])

  assert.deepEqual(groups.map((group) => [group.channelID, group.changes.map((item) => item.id)]), [
    [2, [1, 2, 3]],
    [3, [4]],
  ])
  assert.deepEqual(groups[0].sources.map((source) => [source.sourceName, source.changes.map((item) => item.id)]), [
    ["Key A", [1, 2]],
    ["Key B", [3]],
  ])
})

test("refresh preserves still-pending selections and removes resolved rows", () => {
  assert.deepEqual(reconcileUpstreamGroupChangeSelection([1, 2, 9], [
    change({ id: 1, assignment_state: "awaiting_facts" }),
    change({ id: 2, assignment_state: "ready" }),
  ]), [1, 2])
})

test("batch suggestions are only reusable when every selected change agrees", () => {
  assert.deepEqual(sharedUpstreamGroupChangeSuggestedTargets([
    change({ id: 1, suggested_candidate_group_ids: [3, 1, 3] }),
    change({ id: 2, suggested_candidate_group_ids: [1, 3] }),
  ]), [1, 3])
  assert.equal(sharedUpstreamGroupChangeSuggestedTargets([
    change({ id: 1, suggested_candidate_group_ids: [1] }),
    change({ id: 2, suggested_candidate_group_ids: [2] }),
  ]), null)
  assert.equal(sharedUpstreamGroupChangeSuggestedTargets([
    change({ id: 1, suggested_candidate_group_ids: [] }),
    change({ id: 2, suggested_candidate_group_ids: [2] }),
  ]), null)
  assert.deepEqual(sharedUpstreamGroupChangeSuggestedTargets([change({ suggested_candidate_group_ids: [] })]), [])
  assert.deepEqual(sharedUpstreamGroupChangeSuggestedTargets([]), [])
})

test("suggested targets are grouped once while preserving their first-seen order", () => {
  assert.deepEqual(upstreamGroupChangeSuggestedTargetIDs([
    change({ id: 1, suggested_candidate_group_ids: [7, 3, 7] }),
    change({ id: 2, suggested_candidate_group_ids: [3, 9] }),
  ]), [7, 3, 9])
  assert.deepEqual(upstreamGroupChangeSuggestedTargetIDs([]), [])
})

test("blocked pending changes cannot be selected or retained", () => {
  const blocked = change({ id: 2, assignment_actionable: false, assignment_blocked_reason: "事实尚未同步" })
  assert.equal(upstreamGroupChangeIsActionable(blocked), false)
  assert.deepEqual(reconcileUpstreamGroupChangeSelection([1, 2, 9], [change(), blocked]), [1])
  assert.equal(upstreamGroupChangeSelectionState([blocked], new Set([2])), false)
  assert.equal(upstreamGroupChangeReasonLabel(blocked), "事实尚未同步")
})

test("only quarantined removal intents require manual review", () => {
  assert.equal(upstreamGroupChangeRequiresManualRemovalReview(change({
    kind: "removed",
    status: "ignored",
    assignment_reason_code: "manual_review_required",
  })), true)
  assert.equal(upstreamGroupChangeRequiresManualRemovalReview(change({
    kind: "added",
    status: "ignored",
    assignment_reason_code: "manual_review_required",
  })), false)
  assert.equal(upstreamGroupChangeRequiresManualRemovalReview(change({
    kind: "removed",
    status: "acknowledged",
    assignment_reason_code: "manual_review_required",
  })), false)
})

test("manual removal review exposes the persisted isolation reason without leaking unknown enums", () => {
  assert.equal(upstreamGroupChangeManualReviewReasonLabel(change({
    kind: "removed",
    status: "ignored",
    assignment_reason_code: "manual_review_required",
    removal_manual_review_reason: "remote_identity_changed",
  })), "上游密钥身份已发生变化")
  assert.equal(upstreamGroupChangeManualReviewReasonLabel(change({
    kind: "removed",
    status: "ignored",
    removal_manual_review_reason: "future_internal_reason",
  })), "受管密钥状态需要人工核验")

  const persistedReasons = new Map([
    ["legacy_removal_intent", "旧版删除记录缺少当前安全栅栏"],
    ["inventory_removal_intent_invalid", "上游密钥库存删除记录不完整"],
    ["inventory_removal_identity_invalid", "上游密钥库存删除身份无效"],
    ["inventory_removal_claim_invalid", "上游密钥库存删除声明无效"],
    ["inventory_removal_fence_changed", "上游密钥库存删除栅栏已发生变化"],
    ["remote_name_ambiguous", "上游存在多个同名密钥，无法确认删除目标"],
    ["remote_name_reappeared", "同名上游密钥以新身份重新出现"],
  ])
  for (const [reason, label] of persistedReasons) {
    assert.equal(upstreamGroupChangeManualReviewReasonLabel(change({
      kind: "removed",
      status: "ignored",
      removal_manual_review_reason: reason,
    })), label)
  }
})

test("each visible status group exposes a tri-state selection", () => {
  const changes = [change({ id: 1 }), change({ id: 2 }), change({ id: 3 })]
  assert.equal(upstreamGroupChangeSelectionState(changes, new Set()), false)
  assert.equal(upstreamGroupChangeSelectionState(changes, new Set([1])), "indeterminate")
  assert.equal(upstreamGroupChangeSelectionState(changes, new Set([1, 2, 3])), true)
})

test("history requests carry one bounded server cursor page and every active filter", () => {
  assert.equal(
    upstreamGroupChangeHistoryPath("review", "next-cursor", 500, {
      search: "青色渠道 官渠",
      channel: "7",
      group: "group:22",
      source: "key:10",
    }),
    "/gateway/upstream-group-changes/history?page_size=100&view=review&cursor=next-cursor&search=%E9%9D%92%E8%89%B2%E6%B8%A0%E9%81%93+%E5%AE%98%E6%B8%A0&channel=7&group=group%3A22&source=key%3A10",
  )
  assert.equal(
    upstreamGroupChangeHistoryPath("history", "", 0, {
      search: "",
      channel: upstreamGroupChangeFilterAll,
      group: upstreamGroupChangeFilterAll,
      source: upstreamGroupChangeFilterAll,
    }),
    "/gateway/upstream-group-changes/history?page_size=50",
  )
})

test("history cursor navigation synchronously locks rapid Next and Back clicks", () => {
  const gate = createUpstreamGroupChangeHistoryRequestGate()
  let state = initialUpstreamGroupChangeHistoryCursorState()
  const secondPageKey = "history?view=ignored&cursor=page-2#0"

  if (gate.startNavigation(secondPageKey)) state = nextUpstreamGroupChangeHistoryCursorState(state, "page-2")
  if (gate.startNavigation(secondPageKey)) state = nextUpstreamGroupChangeHistoryCursorState(state, "page-2")

  assert.deepEqual(state, { page: 2, cursor: "page-2", previousCursors: [""] })
  assert.equal(gate.navigationTargetKey(), secondPageKey)

  const secondPageRequest = gate.beginRequest(secondPageKey)
  assert.equal(gate.completeRequest(secondPageRequest, secondPageKey), true)
  assert.equal(gate.navigationTargetKey(), null)

  const firstPageKey = "history?view=ignored#0"
  if (gate.startNavigation(firstPageKey)) state = previousUpstreamGroupChangeHistoryCursorState(state)
  if (gate.startNavigation(firstPageKey)) state = previousUpstreamGroupChangeHistoryCursorState(state)

  assert.deepEqual(state, { page: 1, cursor: "", previousCursors: [] })
})

test("history cursor request token rejects stale completions before they can replace or unlock the active page", () => {
  const gate = createUpstreamGroupChangeHistoryRequestGate()
  const firstPageKey = "history?view=ignored#0"
  const secondPageKey = "history?view=ignored&cursor=page-2#0"
  const firstPageRequest = gate.beginRequest(firstPageKey)

  assert.equal(gate.startNavigation(secondPageKey), true)
  gate.cancelRequest(firstPageRequest)
  const secondPageRequest = gate.beginRequest(secondPageKey)

  assert.equal(gate.matchesCurrentRequest(firstPageRequest, firstPageKey), false)
  assert.equal(gate.completeRequest(firstPageRequest, firstPageKey), false)
  assert.equal(gate.navigationTargetKey(), secondPageKey)
  assert.equal(gate.matchesCurrentRequest(secondPageRequest, secondPageKey), true)
  assert.equal(gate.completeRequest(secondPageRequest, secondPageKey), true)
  assert.equal(gate.navigationTargetKey(), null)

  const repeatFirstPageRequest = gate.beginRequest(firstPageKey)
  assert.equal(gate.matchesCurrentRequest(firstPageRequest, firstPageKey), false)
  assert.equal(gate.matchesCurrentRequest(repeatFirstPageRequest, firstPageKey), true)
})
