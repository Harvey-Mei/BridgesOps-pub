import assert from "node:assert/strict"
import test from "node:test"

import {
  candidateTargetPatchForEdit,
  keyGroupRoutePolicyDraft,
} from "./gateway-key-route-policy"
import type { GatewayKeyGroupRoutePolicy } from "./api-types"

test("moving a manually targeted Key preserves its candidate selection", () => {
  assert.deepEqual(candidateTargetPatchForEdit({
    candidate_group_id: 12,
    candidate_target_source: "manual",
    key_group_id: 3,
  }, 4, "12"), {})
})

test("moving a policy-initialized Key pins its resolved target instead of applying another policy", () => {
  assert.deepEqual(candidateTargetPatchForEdit({
    candidate_group_id: 12,
    candidate_target_source: "key_group_policy",
    key_group_id: 3,
  }, 4, "12"), {
    candidate_group_id: 12,
    candidate_target_mode: "candidate_group",
  })
})

test("an explicit candidate selection change produces the requested target mode", () => {
  const key = { candidate_group_id: 12, candidate_target_source: "manual" as const, key_group_id: 3 }
  assert.deepEqual(candidateTargetPatchForEdit(key, 3, "__policy__"), {
    candidate_group_id: 0,
    candidate_target_mode: "key_group_policy",
  })
  assert.deepEqual(candidateTargetPatchForEdit(key, 3, "__all__"), {
    candidate_group_id: 0,
    candidate_target_mode: "all_candidates",
  })
})

test("policy editor hydrates the existing target, mode and enabled state", () => {
  const policies: GatewayKeyGroupRoutePolicy[] = [{
    id: 9,
    key_group_id: 3,
    gateway_app: "codex",
    model_family: "codex",
    default_candidate_group_id: 27,
    target_mode: "candidate_group",
    enabled: false,
    policy_version: 4,
  }]

  assert.deepEqual(keyGroupRoutePolicyDraft(policies, 3, "codex", "codex", 1), {
    existing: policies[0],
    allCandidates: false,
    candidateGroupID: 27,
    enabled: false,
  })
  assert.deepEqual(keyGroupRoutePolicyDraft(policies, 8, "codex", "codex", 1), {
    existing: null,
    allCandidates: false,
    candidateGroupID: 1,
    enabled: true,
  })
})
