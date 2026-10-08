import assert from "node:assert/strict"
import test from "node:test"

import type { GatewayCandidateGroup, GatewayDownstreamKey, GatewayRouteCandidate } from "./api-types"
import {
  candidateManagementInventoryCandidateIsCurrent,
  candidateManagementInventoryCandidates,
  candidateManagementMemberKey,
  candidateManagementRankedMemberKeys,
  type CandidateManagementCurrentCandidate,
} from "./candidate-management-inventory"
import { buildCandidateGroupMembershipRows } from "./candidate-group-membership"

function downstreamKey(overrides: Partial<GatewayDownstreamKey> = {}) {
  return {
    id: 7,
    name: "团队 Key",
    app: "codex",
    key_prefix: "sk",
    smart_routing: true,
    current_concurrency: 0,
    enabled: true,
    created_at: "2026-07-24T00:00:00Z",
    updated_at: "2026-07-24T00:00:00Z",
    candidate_group_id: 90,
    route_exclusions: [],
    ...overrides,
  } as GatewayDownstreamKey
}

function candidate(overrides: Partial<GatewayRouteCandidate> = {}) {
  return {
    channel_id: 11,
    channel_name: "蓝色渠道",
    channel_type: "newapi",
    gateway_app: "codex",
    group_name: "主力",
    ratio: 1,
    completion_ratio: 1,
    raw_ratio: 1,
    raw_completion_ratio: 1,
    funding_factor: 1,
    cost_basis_version: 1,
    effective_price: 1,
    cache_ratio: 0,
    cache_ratio_known: false,
    model_families: ["codex"],
    model_mapping_families: ["codex"],
    model_ids: ["gpt-5.4"],
    latency_probe_model: "gpt-5.4",
    avg_latency_ms: 100,
    score: 100,
    price_score: 100,
    latency_score: 100,
    latency_samples: 10,
    probe_enabled: true,
    disabled_for_key: false,
    cooling_down: false,
    failure_count: 0,
    cooldown_remaining: 0,
    routing_available: true,
    availability_reasons: [],
    ...overrides,
  } as GatewayRouteCandidate
}

const candidateGroups: GatewayCandidateGroup[] = [{
  id: 90,
  name: "主力候选",
  membership_version: 1,
  created_at: "2026-07-24T00:00:00Z",
  updated_at: "2026-07-24T00:00:00Z",
  members: [
    { id: 1, candidate_group_id: 90, upstream_group_id: 101, presence_generation: 2, channel_id: 11, group_name: "主力" },
    { id: 2, candidate_group_id: 90, upstream_group_id: 102, presence_generation: 3, channel_id: 12, group_name: "备用" },
  ],
}]

test("candidate management keeps the static inventory and multi-protocol projections before a model preview exists", () => {
  const key = downstreamKey({
    route_exclusions: [{
      id: 1,
      downstream_key_id: 7,
      channel_id: 12,
      group_name: "备用",
      created_at: "2026-07-24T00:00:00Z",
      updated_at: "2026-07-24T00:00:00Z",
    }],
  })
  const result = candidateManagementInventoryCandidates(key, candidateGroups, [
    candidate(),
    candidate({ gateway_app: "gemini" }),
    candidate({ channel_id: 12, channel_name: "青色渠道", group_name: "备用", gateway_app: "claude" }),
    candidate({ channel_id: 99, channel_name: "分组外", group_name: "不应出现" }),
  ], [])

  assert.equal(result.length, 3)
  assert.deepEqual(result.map((item) => `${item.channel_id}/${item.group_name}/${item.gateway_app}`), [
    "11/主力/codex",
    "11/主力/gemini",
    "12/备用/claude",
  ])
  assert.equal(result[2].disabled_for_key, true)
  assert.equal(result[2].kind, "current")
})

test("model-qualified preview overlays inventory health without hiding protocols or historical members", () => {
  const result = candidateManagementInventoryCandidates(downstreamKey(), candidateGroups, [
    candidate(),
    candidate({ gateway_app: "gemini" }),
  ], [
    candidate({
      routing_available: false,
      availability_reasons: ["model_not_listed"],
      model_ids: ["gpt-5.4-preview"],
    }),
  ])

  assert.equal(result.length, 3)
  const current = result.filter(candidateManagementInventoryCandidateIsCurrent)
  assert.deepEqual(current.map((item) => item.gateway_app), ["codex", "gemini"])
  assert.deepEqual(current.map((item) => item.model_ids), [["gpt-5.4-preview"], ["gpt-5.4"]])
  assert.deepEqual(current.map((item) => item.availability_reasons), [["model_not_listed"], []])
  const missing = result.find((item) => item.kind === "missing_snapshot")
  assert.ok(missing)
  assert.equal(missing.membership_snapshot_missing, true)
  assert.equal(missing.channel_type, "unknown")
  assert.deepEqual(missing.availability_reasons, ["no_rate_snapshot"])
  assert.equal("effective_price" in missing, false)
})

test("local key exclusion updates apply immediately to the exact channel/group only", () => {
  const key = downstreamKey({
    route_exclusions: [{
      id: 2,
      downstream_key_id: 7,
      channel_id: 11,
      group_name: "主力",
      created_at: "2026-07-24T00:00:00Z",
      updated_at: "2026-07-24T00:00:00Z",
    }],
  })
  const result = candidateManagementInventoryCandidates(key, candidateGroups, [
    candidate(),
    candidate({ channel_id: 12, channel_name: "青色渠道", group_name: "备用" }),
  ], [], new Map([
    [candidateManagementMemberKey(11, "主力"), false],
    [candidateManagementMemberKey(12, "备用"), true],
  ]))

  assert.equal(result[0].disabled_for_key, false)
  assert.equal(result[1].disabled_for_key, true)
})

test("membership grouping preserves the complete candidate inventory type", () => {
  const inventory = candidateManagementInventoryCandidates(downstreamKey(), candidateGroups, [candidate()], [])
  const rows = buildCandidateGroupMembershipRows(inventory)
  const representative = rows[0].candidates.find(candidateManagementInventoryCandidateIsCurrent)
  assert.ok(representative)
  const currentRepresentative: CandidateManagementCurrentCandidate = representative

  assert.equal(currentRepresentative.disabled_for_key, false)
  assert.equal(currentRepresentative.effective_price, 1)
  assert.deepEqual(currentRepresentative.model_ids, ["gpt-5.4"])
})

test("global candidate ranks preserve preview order and append omissions in original inventory order", () => {
  const a = candidate({ channel_id: 21, group_name: "A" })
  const b = candidate({ channel_id: 22, group_name: "B" })
  const outside = candidate({ channel_id: 99, group_name: "outside" })
  const memberKeys = new Set([
    candidateManagementMemberKey(b.channel_id, b.group_name),
    candidateManagementMemberKey(a.channel_id, a.group_name),
  ])

  assert.deepEqual(candidateManagementRankedMemberKeys(memberKeys, [], [a, b, outside]), [
    candidateManagementMemberKey(a.channel_id, a.group_name),
    candidateManagementMemberKey(b.channel_id, b.group_name),
  ])
  assert.deepEqual(candidateManagementRankedMemberKeys(memberKeys, [b, { ...b, gateway_app: "claude" }], [a, b, outside]), [
    candidateManagementMemberKey(b.channel_id, b.group_name),
    candidateManagementMemberKey(a.channel_id, a.group_name),
  ])
})
