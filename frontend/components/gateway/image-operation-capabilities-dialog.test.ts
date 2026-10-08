import assert from "node:assert/strict"
import test from "node:test"
import type {
  GatewayImageOperationCapability,
  GatewayImageOperationTarget,
  GatewayImageOperationTargetInventoryPage,
} from "../../lib/api-types"
import {
  IMAGE_OPERATION_TARGET_MAXIMUM,
  IMAGE_OPERATION_TARGET_PAGE_SIZE,
  loadGatewayImageOperationTargetInventory,
} from "../../lib/image-operation-target-inventory"
import {
  IMAGE_CAPABILITY_RENDER_MAXIMUM,
  IMAGE_CAPABILITY_RENDER_STEP,
  boundedImageCapabilityBatchGroupRows,
  boundedImageCapabilityGroupRows,
  capabilityFormError,
  filterImageCapabilityTargetOptions,
  filterImageCapabilityBatchTargets,
  filterImageOperationCapabilities,
  groupImageCapabilityBatchTargets,
  groupImageOperationCapabilities,
  imageCapabilityBatchRequest,
  imageCapabilityBatchFailure,
  imageCapabilityBatchTargets,
  imageCapabilityChannelOptions,
  imageCapabilityRetryableTargets,
  imageCapabilityStatus,
  imageCapabilityWritesReady,
  runImageCapabilityBatch,
  summarizeImageOperationCapabilities,
  type CapabilityForm,
  type ImageCapabilityTargetOption,
} from "./image-operation-capabilities-dialog"

function capability(overrides: Partial<GatewayImageOperationCapability>): GatewayImageOperationCapability {
  return {
    id: 1,
    channel_id: 10,
    channel_name: "蓝色渠道",
    upstream_group_id: 101,
    group_name: "图像标准组",
    presence_generation: 2,
    model_name: "gpt-image-1",
    operation: "image_generation",
    protocol: "openai_images",
    endpoint_path: "/v1/images/generations",
    task_mode: "unknown",
    idempotency_key_supported: false,
    task_id_returned: false,
    task_query_supported: false,
    task_success_values: [],
    task_failure_values: [],
    evidence_source: "observed_success",
    evidence_detail: "successful_response",
    evidence_observed_at: "2026-07-23T16:00:00Z",
    capability_version: 1,
    recovery_complete: false,
    automatic_replay_allowed: false,
    recovery_blockers: [
      "async_task_mode_not_confirmed",
      "idempotency_key_not_confirmed",
      "task_id_not_confirmed",
      "task_query_not_confirmed",
      "terminal_state_mapping_incomplete",
      "gateway_task_recovery_not_implemented",
    ],
    created_at: "2026-07-23T16:00:00Z",
    updated_at: "2026-07-23T16:00:00Z",
    ...overrides,
  }
}

const items = [
  capability({ id: 1 }),
  capability({
    id: 2,
    model_name: "gpt-image-async",
    task_mode: "async",
    protocol: "openai_responses",
    recovery_blockers: ["idempotency_key_not_confirmed", "gateway_task_recovery_not_implemented"],
  }),
  capability({
    id: 3,
    channel_id: 20,
    channel_name: "阿尔法渠道",
    upstream_group_id: 202,
    group_name: "绘图组",
    model_name: "image-pro",
    task_mode: "async",
    idempotency_key_supported: true,
    idempotency_header: "Idempotency-Key",
    task_id_returned: true,
    task_id_json_path: "id",
    task_query_supported: true,
    task_query_method: "GET",
    task_query_path_template: "/v1/tasks/{task_id}",
    task_status_json_path: "status",
    task_success_values: ["completed"],
    task_failure_values: ["failed"],
    recovery_complete: false,
    recovery_blockers: ["gateway_task_recovery_not_implemented"],
    protocol: "openai_responses",
  }),
]

test("生图能力统计区分待确认、已确认和能力证据完整", () => {
  assert.equal(imageCapabilityStatus(items[0]), "pending")
  assert.equal(imageCapabilityStatus(items[1]), "confirmed")
  assert.equal(imageCapabilityStatus(items[2]), "complete")
  assert.deepEqual(summarizeImageOperationCapabilities(items), {
    total: 3,
    channels: 2,
    pending: 1,
    confirmed: 1,
    complete: 1,
  })
})

test("任务查询允许先保存终态映射未完整的部分能力事实", () => {
  const form: CapabilityForm = {
    channelID: "10",
    expectedUpstreamGroupID: 101,
    expectedPresenceGeneration: 2,
    groupName: "图像标准组",
    modelName: "gpt-image-async",
    protocol: "openai_responses",
    taskMode: "async",
    idempotencyKeySupported: false,
    idempotencyHeader: "Idempotency-Key",
    taskIDReturned: true,
    taskIDJSONPath: "id",
    taskQuerySupported: true,
    taskQueryPathTemplate: "/v1/tasks/{task_id}",
    taskStatusJSONPath: "status",
    taskSuccessValues: "",
    taskFailureValues: "",
    expectedVersion: 1,
  }
  assert.equal(capabilityFormError(form), "")
  assert.equal(capabilityFormError({ ...form, taskSuccessValues: "completed", taskFailureValues: "completed" }), "同一个任务状态不能同时属于成功和失败")
})

test("搜索、渠道、协议和能力状态筛选可以组合", () => {
  assert.deepEqual(
    filterImageOperationCapabilities(items, "async", "confirmed", "openai_responses", "10").map((item) => item.id),
    [2],
  )
  assert.deepEqual(
    filterImageOperationCapabilities(items, "阿尔法", "all", "all", "all").map((item) => item.id),
    [3],
  )
  assert.deepEqual(
    filterImageOperationCapabilities(items, "Images API", "all").map((item) => item.id),
    [1],
  )
})

test("渠道筛选项按名称稳定排序并显示记录数", () => {
  assert.deepEqual(imageCapabilityChannelOptions(items), [
    { value: "20", id: 20, label: "阿尔法渠道", count: 1 },
    { value: "10", id: 10, label: "蓝色渠道", count: 2 },
  ])
})

test("分组视图按渠道或状态生成稳定分组", () => {
  const channelGroups = groupImageOperationCapabilities(items, "channel")
  assert.deepEqual(channelGroups.map((group) => [group.key, group.items.length]), [
    ["channel:20", 1],
    ["channel:10", 2],
  ])
  const statusGroups = groupImageOperationCapabilities(items, "status")
  assert.deepEqual(statusGroups.map((group) => group.key), ["status:pending", "status:confirmed", "status:complete"])
})

test("新增配置目标可以按渠道、分组和模型搜索", () => {
  const options: ImageCapabilityTargetOption[] = [
    { value: "a", label: "蓝色渠道 / 图像标准组 · gpt-image-1", channelID: 10, upstreamGroupID: 101, presenceGeneration: 2, groupName: "图像标准组", modelName: "gpt-image-1" },
    { value: "b", label: "阿尔法渠道 / 绘图组 · image-pro", channelID: 20, upstreamGroupID: 202, presenceGeneration: 3, groupName: "绘图组", modelName: "image-pro" },
  ]
  assert.deepEqual(filterImageCapabilityTargetOptions(options, "20").map((option) => option.value), ["b"])
  assert.deepEqual(filterImageCapabilityTargetOptions(options, "标准组").map((option) => option.value), ["a"])
})

function inventoryTarget(overrides: Partial<GatewayImageOperationTarget> = {}): GatewayImageOperationTarget {
  return {
    channel_id: 10,
    channel_name: "蓝色渠道",
    upstream_group_id: 101,
    group_name: "图像标准组",
    presence_generation: 2,
    model_name: "gpt-image-1",
    models_synced_at: "2026-07-23T15:59:00Z",
    group_facts_version: 4,
    ...overrides,
  }
}

test("批量目标合并多协议投影，保留当前快照未见的能力记录", () => {
  const stale = capability({
    id: 9,
    channel_id: 30,
    channel_name: "历史渠道",
    upstream_group_id: 303,
    group_name: "历史图像组",
    model_name: "old-image",
    protocol: "openai_images",
    capability_version: 7,
  })
  const targets = imageCapabilityBatchTargets([
    inventoryTarget(),
    inventoryTarget({ model_name: "gpt-image-async" }),
  ], [items[0], items[1], items[2], stale], "openai_images")
  assert.deepEqual(targets.map((target) => [target.channelID, target.modelName, target.action, target.expectedVersion, target.currentSnapshot]), [
    [10, "gpt-image-1", "update", 1, true],
    [10, "gpt-image-async", "create", 0, true],
    [30, "old-image", "update", 7, false],
  ])
  assert.deepEqual(filterImageCapabilityBatchTargets(targets, "async", "unconfigured").map((target) => target.modelName), ["gpt-image-async"])
  assert.deepEqual(filterImageCapabilityBatchTargets(targets, "历史", "pending").map((target) => target.modelName), ["old-image"])
  assert.deepEqual(imageCapabilityBatchTargets([], [stale], "openai_images").map((target) => [target.modelName, target.currentSnapshot]), [["old-image", false]])
})

test("批量请求只应用显式配置，保留每个目标的身份和 CAS 版本", () => {
  const target = imageCapabilityBatchTargets([inventoryTarget()], items, "openai_images").find((item) => item.modelName === "gpt-image-1")!
  const request = imageCapabilityBatchRequest(target, {
    protocol: "openai_responses",
    taskMode: "async",
    idempotencyKeySupported: true,
    idempotencyHeader: "Idempotency-Key",
    taskIDReturned: true,
    taskIDJSONPath: "task.id",
    taskQuerySupported: true,
    taskQueryPathTemplate: "/v1/tasks/{task_id}",
    taskStatusJSONPath: "task.status",
    taskSuccessValues: "completed, completed",
    taskFailureValues: "failed",
  })
  assert.deepEqual(request, {
    channel_id: 10,
    expected_upstream_group_id: 101,
    expected_presence_generation: 2,
    group_name: "图像标准组",
    model_name: "gpt-image-1",
    operation: "image_generation",
    protocol: "openai_responses",
    endpoint_path: "/v1/responses",
    task_mode: "async",
    idempotency_key_supported: true,
    idempotency_header: "Idempotency-Key",
    task_id_returned: true,
    task_id_json_path: "task.id",
    task_query_supported: true,
    task_query_method: "GET",
    task_query_path_template: "/v1/tasks/{task_id}",
    task_status_json_path: "task.status",
    task_success_values: ["completed"],
    task_failure_values: ["failed"],
    expected_version: 1,
  })
})

test("批量执行限制并发，并逐项保留 CAS 冲突", async () => {
  const targets = imageCapabilityBatchTargets([
    inventoryTarget(),
    inventoryTarget({ model_name: "gpt-image-async" }),
  ], items, "openai_images")
  let active = 0
  let maxActive = 0
  const result = await runImageCapabilityBatch(targets, async (target) => {
    active += 1
    maxActive = Math.max(maxActive, active)
    await new Promise((resolve) => setTimeout(resolve, 2))
    active -= 1
    if (target.modelName === "gpt-image-1") throw new Error("image operation capability version changed")
    return target.modelName
  }, { concurrency: 2 })
  assert.ok(maxActive <= 2)
  assert.equal(result.succeeded.length, 1)
  assert.deepEqual(result.failed.map((failure) => [failure.target.modelName, failure.kind]), [["gpt-image-1", "conflict"]])
})

test("取消批量执行不会把未发送目标伪装成成功", async () => {
  const targets = imageCapabilityBatchTargets([
    inventoryTarget(),
    inventoryTarget({ model_name: "gpt-image-async" }),
  ], items, "openai_images")
  const controller = new AbortController()
  const result = await runImageCapabilityBatch(targets, async () => {
    controller.abort()
    throw Object.assign(new Error("aborted"), { name: "AbortError" })
  }, { concurrency: 1, signal: controller.signal })
  assert.equal(result.succeeded.length, 0)
  assert.equal(result.cancelled, true)
  assert.deepEqual(result.failed.map((failure) => failure.kind), ["cancelled", "cancelled"])
})

test("批量错误分类不会把代次变化、目标消失和 CAS 冲突纳入自动重试", () => {
  const target = imageCapabilityBatchTargets([inventoryTarget()], items, "openai_images")[0]
  const conflict = imageCapabilityBatchFailure(target, new Error("image operation capability version changed"))
  const generation = imageCapabilityBatchFailure(target, new Error("upstream group changed; refresh capability data"))
  const missing = imageCapabilityBatchFailure(target, new Error("upstream group is not currently present"))
  const failed = imageCapabilityBatchFailure(target, new Error("HTTP 500"))
  assert.deepEqual([conflict.kind, generation.kind, missing.kind, failed.kind], ["conflict", "generation_changed", "missing", "failed"])
  assert.deepEqual(imageCapabilityRetryableTargets({ succeeded: [], failed: [conflict, generation, missing, failed], cancelled: false }), [target])
})

test("同名分组换代会产生新目标身份并移除旧代选择键", () => {
  const targets = imageCapabilityBatchTargets([
    inventoryTarget({ presence_generation: 3 }),
  ], [items[0]], "openai_images", "snapshot-new-generation")
  const current = targets.find((target) => target.currentSnapshot)!
  const stale = targets.find((target) => !target.currentSnapshot)!

  assert.equal(current.action, "create")
  assert.equal(current.presenceGeneration, 3)
  assert.equal(stale.action, "update")
  assert.equal(stale.presenceGeneration, 2)
  assert.notEqual(current.key, stale.key)
  assert.equal(new Set([stale.key]).has(current.key), false)
})

test("目标或能力数据不完整时写入门禁保持关闭", () => {
  const readyInventory = {
    data: {
      items: [inventoryTarget()],
      snapshot: "snapshot-ready",
      capturedAt: "2026-07-23T16:00:00Z",
    },
    loading: false,
    refreshing: false,
    error: null,
    fresh: true,
  }
  assert.equal(imageCapabilityWritesReady(readyInventory, false, false, null, true), true)

  const blockedInventories = [
    { ...readyInventory, loading: true },
    { ...readyInventory, refreshing: true },
    { ...readyInventory, error: "failed" },
    { ...readyInventory, fresh: false },
    { ...readyInventory, data: null },
  ]
  let patchCalls = 0
  for (const inventory of blockedInventories) {
    if (imageCapabilityWritesReady(inventory, false, false, null, true)) patchCalls += 1
  }
  for (const state of [
    { loading: true, refreshing: false, error: null, hasData: true },
    { loading: false, refreshing: true, error: null, hasData: true },
    { loading: false, refreshing: false, error: "failed", hasData: true },
    { loading: false, refreshing: false, error: null, hasData: false },
  ]) {
    if (imageCapabilityWritesReady(readyInventory, state.loading, state.refreshing, state.error, state.hasData)) patchCalls += 1
  }
  assert.equal(patchCalls, 0)
})

function largeInventoryTargets(count: number) {
  return Array.from({ length: count }, (_, index) => {
    const channelID = Math.floor(index / 8) + 1
    return inventoryTarget({
      channel_id: channelID,
      channel_name: `Channel ${channelID}`,
      upstream_group_id: 10_000 + channelID,
      group_name: `group-${channelID}`,
      presence_generation: 1,
      model_name: `image-model-${index % 8}`,
      group_facts_version: 1,
    })
  })
}

test("500 渠道 4000 条能力记录保持分组头并限制主列表渲染", () => {
  const capabilities = largeInventoryTargets(4_000).map((target, index) => capability({
    id: index + 1,
    channel_id: target.channel_id,
    channel_name: target.channel_name,
    upstream_group_id: target.upstream_group_id,
    group_name: target.group_name,
    presence_generation: target.presence_generation,
    model_name: target.model_name,
  }))
  const groups = groupImageOperationCapabilities(capabilities, "channel")
  const collapsed = new Set(groups.map((group) => group.key))
  const defaultRows = boundedImageCapabilityGroupRows(groups, new Set(), false, IMAGE_CAPABILITY_RENDER_STEP)
  const maximumRows = boundedImageCapabilityGroupRows(groups, new Set(), false, Number.MAX_SAFE_INTEGER)
  const collapsedRows = boundedImageCapabilityGroupRows(groups, collapsed, false)
  const searchRows = boundedImageCapabilityGroupRows(groups, collapsed, true)

  assert.equal(groups.length, 500)
  assert.equal(defaultRows.groups.length, 500)
  assert.equal(defaultRows.renderedItems, IMAGE_CAPABILITY_RENDER_STEP)
  assert.equal(maximumRows.renderedItems, IMAGE_CAPABILITY_RENDER_MAXIMUM)
  assert.equal(collapsedRows.renderedItems, 0)
  assert.equal(searchRows.renderedItems, IMAGE_CAPABILITY_RENDER_STEP)
})

test("500 渠道 4000 个批量目标保持分组头并限制目标行渲染", () => {
  const targets = imageCapabilityBatchTargets(largeInventoryTargets(4_000), [], "openai_images", "snapshot-large")
  const groups = groupImageCapabilityBatchTargets(targets)
  const collapsed = new Set(groups.map((group) => group.channelID))
  const defaultRows = boundedImageCapabilityBatchGroupRows(groups, new Set(), false, IMAGE_CAPABILITY_RENDER_STEP)
  const maximumRows = boundedImageCapabilityBatchGroupRows(groups, new Set(), false, Number.MAX_SAFE_INTEGER)
  const collapsedRows = boundedImageCapabilityBatchGroupRows(groups, collapsed, false)
  const searchRows = boundedImageCapabilityBatchGroupRows(groups, collapsed, true)

  assert.equal(groups.length, 500)
  assert.equal(defaultRows.groups.length, 500)
  assert.equal(defaultRows.renderedItems, IMAGE_CAPABILITY_RENDER_STEP)
  assert.equal(maximumRows.renderedItems, IMAGE_CAPABILITY_RENDER_MAXIMUM)
  assert.equal(collapsedRows.renderedItems, 0)
  assert.equal(searchRows.renderedItems, IMAGE_CAPABILITY_RENDER_STEP)
})

test("独立目标清单完整加载 4000 条同快照分页后才发布", async () => {
  const source = largeInventoryTargets(4_000)
  let calls = 0
  const result = await loadGatewayImageOperationTargetInventory(async (path) => {
    calls += 1
    const url = new URL(path, "https://bridgeops.invalid")
    assert.equal(url.searchParams.get("page_size"), String(IMAGE_OPERATION_TARGET_PAGE_SIZE))
    const offset = Number(url.searchParams.get("cursor") ?? 0)
    const nextOffset = offset + IMAGE_OPERATION_TARGET_PAGE_SIZE
    return {
      items: source.slice(offset, nextOffset),
      next_cursor: nextOffset < source.length ? String(nextOffset) : undefined,
      snapshot: "snapshot-4000",
      captured_at: "2026-07-23T16:00:00Z",
    }
  })

  assert.equal(calls, 16)
  assert.equal(result.items.length, 4_000)
  assert.equal(result.snapshot, "snapshot-4000")
})

test("目标清单分页快照变化时不发布部分结果", async () => {
  let published: Awaited<ReturnType<typeof loadGatewayImageOperationTargetInventory>> | null = null
  await assert.rejects(async () => {
    published = await loadGatewayImageOperationTargetInventory(async (path) => {
      const cursor = new URL(path, "https://bridgeops.invalid").searchParams.get("cursor")
      return {
        items: [inventoryTarget({ model_name: cursor ? "image-second" : "image-first" })],
        next_cursor: cursor ? undefined : "second-page",
        snapshot: cursor ? "snapshot-new" : "snapshot-old",
        captured_at: "2026-07-23T16:00:00Z",
      }
    })
  }, /加载期间发生变化/)
  assert.equal(published, null)
})

test("目标清单拒绝重复身份、循环游标和浏览器上限溢出", async (t) => {
  await t.test("重复身份", async () => {
    await assert.rejects(
      loadGatewayImageOperationTargetInventory(async () => ({
        items: [inventoryTarget(), inventoryTarget()],
        snapshot: "snapshot-duplicate",
        captured_at: "2026-07-23T16:00:00Z",
      })),
      /重复目标/,
    )
  })

  await t.test("循环游标", async () => {
    await assert.rejects(
      loadGatewayImageOperationTargetInventory(async (path) => {
        const cursor = new URL(path, "https://bridgeops.invalid").searchParams.get("cursor")
        return {
          items: [inventoryTarget({ model_name: cursor ? "image-second" : "image-first" })],
          next_cursor: "same-cursor",
          snapshot: "snapshot-loop",
          captured_at: "2026-07-23T16:00:00Z",
        }
      }),
      /游标未向前推进/,
    )
  })

  await t.test("数量上限", async () => {
    let pages = 0
    await assert.rejects(
      loadGatewayImageOperationTargetInventory(async (path): Promise<GatewayImageOperationTargetInventoryPage> => {
        pages += 1
        const offset = Number(new URL(path, "https://bridgeops.invalid").searchParams.get("cursor") ?? 0)
        return {
          items: Array.from({ length: IMAGE_OPERATION_TARGET_PAGE_SIZE }, (_, index) => inventoryTarget({
            channel_id: offset + index + 1,
            channel_name: `Channel ${offset + index + 1}`,
            upstream_group_id: offset + index + 1,
            group_name: `group-${offset + index + 1}`,
            model_name: "image-model",
          })),
          next_cursor: String(offset + IMAGE_OPERATION_TARGET_PAGE_SIZE),
          snapshot: "snapshot-maximum",
          captured_at: "2026-07-23T16:00:00Z",
        }
      }),
      /超过浏览器安全上限/,
    )
    assert.equal(pages, Math.floor(IMAGE_OPERATION_TARGET_MAXIMUM / IMAGE_OPERATION_TARGET_PAGE_SIZE) + 1)
  })
})
