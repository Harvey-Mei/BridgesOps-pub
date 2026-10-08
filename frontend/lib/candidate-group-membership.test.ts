import assert from "node:assert/strict"
import test from "node:test"

import {
  buildCandidateGroupMembershipRows,
  candidateGroupMemberKey,
  candidateGroupMembershipBulkTargets,
  candidateGroupMembershipChannelOptions,
  candidateGroupMembershipFilterAll,
  candidateGroupMembershipProtocolOptions,
  candidateGroupMembershipRowState,
  filterCandidateGroupMembershipRows,
  groupCandidateGroupMembershipRows,
  resolveCandidateGroupMembershipSelection,
  type CandidateGroupMembershipCandidate,
  type CandidateGroupMembershipFilters,
} from "./candidate-group-membership"

const candidates: CandidateGroupMembershipCandidate[] = [
  {
    channel_id: 20,
    channel_name: "蓝色渠道",
    channel_type: "newapi",
    group_name: "生图",
    gateway_app: "gemini",
    routing_available: false,
    availability_reasons: ["gateway_app_mismatch"],
  },
  {
    channel_id: 10,
    channel_name: "青色渠道",
    channel_type: "sub2api",
    group_name: "Claude 官渠",
    gateway_app: "claude",
    cooling_down: true,
    routing_available: false,
    availability_reasons: ["routing_cooldown"],
  },
  {
    channel_id: 20,
    channel_name: "蓝色渠道",
    channel_type: "newapi",
    group_name: "生图",
    gateway_app: "codex",
    routing_available: true,
  },
  {
    channel_id: 20,
    channel_name: "蓝色渠道",
    channel_type: "newapi",
    group_name: "文本",
    gateway_app: "codex",
    routing_available: true,
  },
]

const allFilters: CandidateGroupMembershipFilters = {
  search: "",
  status: candidateGroupMembershipFilterAll,
  protocol: candidateGroupMembershipFilterAll,
  channelID: candidateGroupMembershipFilterAll,
}

test("membership rows aggregate duplicate protocol projections and remain stably grouped", () => {
  const rows = buildCandidateGroupMembershipRows(candidates)
  assert.equal(rows.length, 3)
  assert.deepEqual(rows.map((row) => row.key), [
    candidateGroupMemberKey(20, "生图"),
    candidateGroupMemberKey(20, "文本"),
    candidateGroupMemberKey(10, "Claude 官渠"),
  ])
  assert.deepEqual(rows[0].gatewayApps, ["codex", "gemini"])
  assert.equal(rows[0].routingAvailable, true)
  assert.deepEqual(candidateGroupMembershipRowState(rows[0]), {
    coolingDown: false,
    routingAvailable: true,
    routingUnavailable: true,
    snapshotMissing: false,
    availabilityReasons: ["gateway_app_mismatch"],
  })
  assert.equal(candidateGroupMembershipRowState(rows[0], "codex").routingUnavailable, false)
  assert.deepEqual(candidateGroupMembershipProtocolOptions(rows), ["claude", "codex", "gemini"])
  assert.deepEqual(candidateGroupMembershipChannelOptions(rows).map((option) => option.label), ["蓝色渠道", "青色渠道"])
  assert.deepEqual(groupCandidateGroupMembershipRows(rows).map((channel) => channel.rows.length), [2, 1])
})

test("search, channel, protocol, and status filters compose", () => {
  const rows = buildCandidateGroupMembershipRows(candidates)
  assert.deepEqual(filterCandidateGroupMembershipRows(rows, {
    ...allFilters,
    protocol: "codex",
    status: "routing_available",
  }).map((row) => row.groupName), ["生图", "文本"])

  assert.deepEqual(filterCandidateGroupMembershipRows(rows, {
    ...allFilters,
    protocol: "gemini",
    status: "routing_unavailable",
  }).map((row) => row.groupName), ["生图"])

  assert.deepEqual(filterCandidateGroupMembershipRows(rows, {
    ...allFilters,
    status: "cooling_down",
  }).map((row) => row.groupName), ["Claude 官渠"])
})

test("legacy probe pause facts do not create a health state or routing blocker", () => {
  const rows = buildCandidateGroupMembershipRows([{
    channel_id: 20,
    group_name: "legacy",
    availability_reasons: ["probe_disabled"],
  }])
  assert.equal(rows[0].routingAvailable, true)
  assert.deepEqual(rows[0].availabilityReasons, [])
  assert.deepEqual(candidateGroupMembershipRowState(rows[0]), {
    coolingDown: false,
    routingAvailable: true,
    routingUnavailable: false,
    snapshotMissing: false,
    availabilityReasons: [],
  })
})

test("bulk previews only include rows whose selection would change", () => {
  const rows = buildCandidateGroupMembershipRows(candidates)
  const selected = new Set([rows[0].key, rows[2].key, "999::已消失分组"])
  assert.deepEqual(candidateGroupMembershipBulkTargets(rows, selected, "add").map((row) => row.key), [rows[1].key])
  assert.deepEqual(candidateGroupMembershipBulkTargets(rows, selected, "remove").map((row) => row.key), [rows[0].key, rows[2].key])
  assert.equal(selected.has("999::已消失分组"), true)
})

test("missing snapshot members stay visible as removable unavailable rows", () => {
  const rows = buildCandidateGroupMembershipRows([{
    channel_id: 99,
    channel_name: "渠道 99",
    group_name: "旧分组",
    membership_snapshot_missing: true,
    routing_available: false,
  }])
  assert.equal(rows.length, 1)
  assert.equal(rows[0].snapshotMissing, true)
  assert.equal(candidateGroupMembershipRowState(rows[0]).snapshotMissing, true)
  assert.deepEqual(filterCandidateGroupMembershipRows(rows, {
    ...allFilters,
    status: "routing_unavailable",
  }), rows)
})

test("selection resolution preserves existing members missing from the current snapshot", () => {
  const existingMembers = [{ channel_id: 99, group_name: "旧分组" }]
  const currentCandidates = [{ channel_id: 20, group_name: "当前分组" }]
  const selected = new Set([
    candidateGroupMemberKey(99, "旧分组"),
    candidateGroupMemberKey(20, "当前分组"),
    candidateGroupMemberKey(77, "未知分组"),
  ])
  assert.deepEqual(resolveCandidateGroupMembershipSelection(existingMembers, currentCandidates, selected), {
    members: [existingMembers[0], currentCandidates[0]],
    unresolvedKeys: [candidateGroupMemberKey(77, "未知分组")],
  })
})

test("canonical member keys merge existing and current group names that only differ by case", () => {
  const existingMembers = [{ channel_id: 88, group_name: " Claude 官渠 " }]
  const currentCandidates = [{ channel_id: 88, group_name: "claude 官渠" }]
  const selected = new Set([candidateGroupMemberKey(88, "CLAUDE 官渠")])

  assert.equal(candidateGroupMemberKey(88, " Claude 官渠 "), candidateGroupMemberKey(88, "claude 官渠"))
  assert.deepEqual(resolveCandidateGroupMembershipSelection(existingMembers, currentCandidates, selected), {
    members: [currentCandidates[0]],
    unresolvedKeys: [],
  })

  const rows = buildCandidateGroupMembershipRows([
    { channel_id: existingMembers[0].channel_id, group_name: existingMembers[0].group_name },
    currentCandidates[0],
  ])
  assert.equal(rows.length, 1)
  assert.equal(rows[0].candidates.length, 2)
  assert.equal(candidateGroupMembershipBulkTargets(rows, selected, "add").length, 0)
})

test("selection resolution carries the current upstream generation into the save payload", () => {
  const current = {
    channel_id: 42,
    group_name: "current",
    upstream_group_id: 420,
    presence_generation: 7,
  }
  assert.deepEqual(resolveCandidateGroupMembershipSelection([], [current], new Set([
    candidateGroupMemberKey(current.channel_id, current.group_name),
  ])), {
    members: [current],
    unresolvedKeys: [],
  })
})

test("large inventories remain deterministically grouped and filterable", () => {
  const largeCandidates = Array.from({ length: 500 }).flatMap((_, channelIndex) => (
    Array.from({ length: 4 }).flatMap((__, groupIndex) => ["codex", "claude"].map((gatewayApp) => ({
      channel_id: channelIndex + 1,
      channel_name: `渠道 ${String(channelIndex + 1).padStart(3, "0")}`,
      channel_type: "newapi",
      group_name: `分组 ${groupIndex + 1}`,
      gateway_app: gatewayApp,
      routing_available: groupIndex % 2 === 0,
    })))
  ))
  assert.equal(largeCandidates.length, 4_000)
  const rows = buildCandidateGroupMembershipRows(largeCandidates)
  assert.equal(rows.length, 2_000)
  assert.equal(groupCandidateGroupMembershipRows(rows).length, 500)
  assert.equal(filterCandidateGroupMembershipRows(rows, {
    ...allFilters,
    channelID: "250",
    protocol: "claude",
    status: "routing_available",
  }).length, 2)
})
