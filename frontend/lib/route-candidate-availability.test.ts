import assert from "node:assert/strict"
import test from "node:test"

import {
  candidateAvailabilityLabel,
  candidateAvailabilityReasons,
  candidateGroupSelectionSupportsMappingFamily,
  candidateSupportsMappingFamily,
  filterCandidateGroupMembershipCandidates,
  manualMappedTargetSelectionState,
  manualRouteCandidateBlockingReasons,
  manualRouteCandidateBlockingReasonText,
  manualRouteCandidateSelectable,
} from "./route-candidate-availability"

test("candidate-group membership is gated only by model family", () => {
  const candidate = { model_mapping_families: ["codex"] }
  assert.equal(candidateSupportsMappingFamily(candidate, ""), true)
  assert.equal(candidateSupportsMappingFamily(candidate, " CODEX "), true)
  assert.equal(candidateSupportsMappingFamily(candidate, "claude"), false)
})

test("candidate-group radar family requires any selected member, not every member", () => {
  const selected = [
    { model_mapping_families: ["codex"] },
    { model_mapping_families: ["claude"] },
    { model_mapping_families: [] },
  ]
  assert.equal(candidateGroupSelectionSupportsMappingFamily(selected, "codex"), true)
  assert.equal(candidateGroupSelectionSupportsMappingFamily(selected, "claude"), true)
  assert.equal(candidateGroupSelectionSupportsMappingFamily(selected, ""), true)
  assert.equal(candidateGroupSelectionSupportsMappingFamily(selected.slice(1), "codex"), false)
})

test("candidate-group search keeps every model family visible", () => {
  const candidates = [
    { channel_name: "蓝色渠道", group_name: "生图", gateway_app: "codex" as const, model_mapping_families: [] },
    { channel_name: "青色渠道", group_name: "Claude 官渠", gateway_app: "claude" as const, model_mapping_families: ["claude"] },
  ]
  assert.deepEqual(filterCandidateGroupMembershipCandidates(candidates, ""), candidates)
  assert.deepEqual(filterCandidateGroupMembershipCandidates(candidates, "生图"), [candidates[0]])
  assert.deepEqual(filterCandidateGroupMembershipCandidates(candidates, "claude"), [candidates[1]])
})

test("candidate availability reasons are normalized and ignore legacy probe pause", () => {
  assert.deepEqual(candidateAvailabilityReasons({
    availability_reasons: [" channel_missing ", "", "channel_missing", "probe_disabled", "models_not_synced"],
  }), ["channel_missing", "models_not_synced"])
})

test("fixed routing ignores smart-routing health, cooldown, and probe state", () => {
  assert.equal(manualRouteCandidateSelectable({
    disabled_for_key: true,
    cooling_down: true,
    routing_available: false,
    availability_reasons: [
      "monitor_disabled",
      "channel_error",
      "low_balance",
      "no_rate_snapshot",
      "probe_disabled",
      "routing_cooldown",
      "models_not_synced",
      "model_family_mismatch",
      "model_capability_unknown",
      "no_supported_gateway_app",
      "upstream_temporarily_unavailable",
    ],
  }), true)
  assert.deepEqual(manualRouteCandidateBlockingReasons({
    availability_reasons: ["channel_error", "models_not_synced", "probe_disabled", "routing_cooldown"],
  }), [])
})

test("non-structural health state never becomes a fixed-route blocker", () => {
  const candidate = { availability_reasons: ["channel_error"] }
  assert.equal(manualRouteCandidateSelectable(candidate), true)
  assert.deepEqual(manualRouteCandidateBlockingReasons(candidate), [])
  assert.equal(manualRouteCandidateBlockingReasonText(candidate), "")
})

test("missing rate snapshots remain a fixed-route warning, not a structural blocker", () => {
  const candidate = { availability_reasons: ["no_rate_snapshot"] }
  assert.equal(manualRouteCandidateSelectable(candidate), true)
  assert.deepEqual(manualRouteCandidateBlockingReasons(candidate), [])
  assert.equal(manualRouteCandidateBlockingReasonText(candidate), "")
  assert.equal(candidateAvailabilityLabel("no_rate_snapshot"), "暂无倍率快照")
})

test("only manual-target structural blockers prevent a fixed route", () => {
  const candidate = {
    availability_reasons: [
      "channel_missing",
      "no_rate_snapshot",
      "no_routable_group",
      "upstream_key_missing",
      "upstream_key_disabled",
      "upstream_key_unavailable",
      "upstream_generation_unavailable",
      "no_supported_gateway_app",
      "gateway_app_mismatch",
      "model_family_mismatch",
      "model_not_listed",
      "model_not_supported",
      "provider_contract_changed",
    ],
  }
  assert.equal(manualRouteCandidateSelectable(candidate), false)
  assert.equal(
    manualRouteCandidateBlockingReasonText(candidate),
    "渠道已不存在、渠道 Key 未就绪、渠道 Key 已停用、渠道 Key 不可用、上游分组已变更，请刷新后重新选择、协议不匹配",
  )
  assert.equal(candidateAvailabilityLabel("no_rate_snapshot"), "暂无倍率快照")
  assert.equal(candidateAvailabilityLabel("provider_contract_changed"), "provider_contract_changed")
})

test("requested or recent preview model facts do not block selecting a fixed target", () => {
  const candidate = {
    availability_reasons: ["model_not_listed", "model_not_supported"],
  }
  assert.equal(manualRouteCandidateSelectable(candidate), true)
  assert.deepEqual(manualRouteCandidateBlockingReasons(candidate), [])
  assert.equal(manualRouteCandidateBlockingReasonText(candidate), "")
})

test("only a server-confirmed mapped target may opt into model capability blocking", () => {
  const candidate = {
    availability_reasons: ["model_not_listed", "model_not_supported"],
  }
  assert.equal(manualRouteCandidateSelectable(candidate), true)
  assert.equal(manualRouteCandidateSelectable(candidate, { enforceServerConfirmedMappedModel: true }), false)
  assert.equal(
    manualRouteCandidateBlockingReasonText(candidate, { enforceServerConfirmedMappedModel: true }),
    "不支持当前模型、模型已确认不支持",
  )
})

test("server-owned mapped target blockers merge by channel and canonical group", () => {
  const state = manualMappedTargetSelectionState({
    channel_id: 17,
    group_name: " Primary ",
    availability_reasons: ["routing_cooldown", "model_not_supported"],
  }, {
    mapping_state: "resolved",
    gate_provenance: "manual_mapped_model_gate",
    targets: [{
      candidate_member_id: 9,
      channel_id: 17,
      group_name: "primary",
      structural_blocked: true,
      manual_mapped_model_gate: true,
      gate_provenance: "manual_mapped_model_gate",
      reason: "model_not_listed",
    }],
  })

  assert.equal(state.selectable, false)
  assert.deepEqual(state.blockingReasons, ["model_not_listed"])
  assert.equal(state.target?.candidate_member_id, 9)
})

test("generic requested-model facts cannot manufacture a mapped-target blocker", () => {
  const state = manualMappedTargetSelectionState({
    channel_id: 17,
    group_name: "primary",
    availability_reasons: ["model_not_listed", "model_not_supported"],
  }, {
    mapping_state: "resolved",
    gate_provenance: "manual_mapped_model_gate",
    targets: [{
      candidate_member_id: 9,
      channel_id: 17,
      group_name: "PRIMARY",
      structural_blocked: false,
      manual_mapped_model_gate: true,
      gate_provenance: "manual_mapped_model_gate",
    }],
  })

  assert.equal(state.selectable, true)
  assert.deepEqual(state.blockingReasons, [])
})

test("a resolved mapped preview must contain the exact candidate target", () => {
  const state = manualMappedTargetSelectionState({
    channel_id: 17,
    group_name: "primary",
    availability_reasons: ["model_not_listed"],
  }, {
    mapping_state: "resolved",
    gate_provenance: "manual_mapped_model_gate",
    targets: [],
  })

  assert.equal(state.selectable, false)
  assert.deepEqual(state.blockingReasons, ["manual_mapped_target_blocked"])
})

test("unresolved mapping and fixed-route health warnings remain selectable", () => {
  const state = manualMappedTargetSelectionState({
    channel_id: 22,
    group_name: "vision",
    disabled_for_key: true,
    cooling_down: true,
    availability_reasons: ["routing_cooldown", "probe_disabled", "no_rate_snapshot", "model_not_listed"],
  }, {
    mapping_state: "unresolved",
    gate_provenance: "mapping_unresolved",
    targets: [],
  })

  assert.equal(state.mappingUnresolved, true)
  assert.equal(state.selectable, true)
  assert.deepEqual(state.blockingReasons, [])
})
