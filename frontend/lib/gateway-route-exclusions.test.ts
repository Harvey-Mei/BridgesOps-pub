import assert from "node:assert/strict"
import test from "node:test"
import {
  gatewayRouteExclusionSnapshotCopyText,
  gatewayRouteExclusionSnapshotState,
} from "./gateway-route-exclusions"

test("keeps version-zero history unavailable without consulting a supplied current snapshot", () => {
  const log = {
    route_exclusion_snapshot_version: 0,
    route_exclusion_snapshot: {
      version: 1,
      captured_at: "2026-07-24T10:00:00Z",
      total_entries: 1,
      truncated: false,
      entries: [{
        channel_id: 7,
        group_name: "sensitive-current-group",
        reason: "upstream_temporarily_unavailable",
        upstream_group_id: 12,
        presence_generation: 4,
        failure_count: 3,
        cooldown_until: "2026-07-24T10:30:00Z",
      }],
    },
  }

  assert.equal(gatewayRouteExclusionSnapshotState(log).kind, "not_collected")
  const copy = gatewayRouteExclusionSnapshotCopyText(log)
  assert.match(copy, /未采集 \/ N\/A/)
  assert.match(copy, /未使用当前状态回填/)
  assert.equal(copy.includes("sensitive-current-group"), false)
})

test("formats generation-bound indefinite isolation facts and truncated coverage from version one", () => {
  const log = {
    route_exclusion_snapshot_version: 1,
    route_exclusion_snapshot: {
      version: 1,
      captured_at: "2026-07-24T10:00:00Z",
      total_entries: 70,
      truncated: true,
      entries: [{
        channel_id: 7,
        group_name: "claude",
        reason: "upstream_temporarily_unavailable",
        upstream_group_id: 12,
        presence_generation: 4,
		failure_count: 3,
      }, {
        channel_id: 8,
        group_name: "images",
        reason: "upstream_generation_unavailable",
      }],
    },
  }

  assert.equal(gatewayRouteExclusionSnapshotState(log).kind, "captured")
  assert.equal(gatewayRouteExclusionSnapshotCopyText(log), [
    "候选排除快照",
    "采集状态：已采集（v1）",
    "采集时间：2026-07-24T10:00:00Z",
    "覆盖范围：已保存 2 / 70 条（已截断）",
    "排除 1：渠道 #7 / claude",
	"  原因：上游暂不可用（故障隔离）",
	"  分组代际：上游分组 #12 · generation 4",
	"  冷却事实：失败计数 3 · 等待连续三次探测成功",
    "排除 2：渠道 #8 / images",
    "  原因：上游分组 generation 不可用",
    "  分组代际：上游分组 N/A · generation N/A",
  ].join("\n"))
})

test("rejects malformed or unknown snapshot facts without echoing them", () => {
  const log = {
    route_exclusion_snapshot_version: 1,
    route_exclusion_snapshot: {
      version: 1,
      captured_at: "2026-07-24T10:00:00Z",
      total_entries: 1,
      truncated: false,
      entries: [{
        channel_id: 7,
        group_name: "claude",
        reason: "raw provider error: credential leaked",
      }],
    },
  }

  assert.equal(gatewayRouteExclusionSnapshotState(log).kind, "unavailable")
  const copy = gatewayRouteExclusionSnapshotCopyText(log)
  assert.match(copy, /快照不可用 \/ N\/A/)
  assert.equal(copy.includes("credential leaked"), false)
})

test("rejects control characters in route exclusion group names", () => {
  const log = {
    route_exclusion_snapshot_version: 1,
    route_exclusion_snapshot: {
      version: 1,
      captured_at: "2026-07-24T10:00:00Z",
      total_entries: 1,
      truncated: false,
      entries: [{
        channel_id: 7,
        group_name: "claude\u0000hidden",
        reason: "upstream_generation_unavailable",
      }],
    },
  }

  assert.equal(gatewayRouteExclusionSnapshotState(log).kind, "unavailable")
  assert.equal(gatewayRouteExclusionSnapshotCopyText(log).includes("hidden"), false)
})
