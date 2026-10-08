import type { GatewayDownstreamKey, GatewayKeyGroupRoutePolicy } from "./api-types"

export type GatewayCandidateSelection = "__policy__" | "__all__" | `${number}`

export interface GatewayKeyGroupRoutePolicyDraft {
  existing: GatewayKeyGroupRoutePolicy | null
  allCandidates: boolean
  candidateGroupID: number
  enabled: boolean
}

export function candidateSelectionForKey(key: Pick<GatewayDownstreamKey, "candidate_group_id">): GatewayCandidateSelection {
  return key.candidate_group_id ? String(key.candidate_group_id) as `${number}` : "__all__"
}

export function candidateTargetMode(selection: string) {
  if (selection === "__policy__") return "key_group_policy"
  if (selection === "__all__") return "all_candidates"
  return "candidate_group"
}

export function candidateTargetPatchForEdit(
  key: Pick<GatewayDownstreamKey, "candidate_group_id" | "candidate_target_source" | "key_group_id">,
  nextKeyGroupID: number,
  nextSelection: string,
): Record<string, unknown> {
  const currentSelection = candidateSelectionForKey(key)
  const keyGroupChanged = nextKeyGroupID !== Number(key.key_group_id ?? 0)
  const movedPolicySnapshot = keyGroupChanged && key.candidate_target_source === "key_group_policy"
  if (nextSelection === currentSelection && !movedPolicySnapshot) return {}

  return {
    candidate_group_id: nextSelection.startsWith("__") ? 0 : Number(nextSelection),
    candidate_target_mode: candidateTargetMode(nextSelection),
  }
}

export function keyGroupRoutePolicyDraft(
  policies: GatewayKeyGroupRoutePolicy[],
  keyGroupID: number,
  gatewayApp: string,
  modelFamily: string,
  fallbackCandidateGroupID: number,
): GatewayKeyGroupRoutePolicyDraft {
  const family = modelFamily.trim().toLowerCase()
  const existing = policies.find((policy) => (
    policy.key_group_id === keyGroupID
    && policy.gateway_app === gatewayApp
    && policy.model_family === family
  )) ?? null

  return {
    existing,
    allCandidates: existing?.target_mode === "all_candidates",
    candidateGroupID: existing?.default_candidate_group_id ?? fallbackCandidateGroupID,
    enabled: existing?.enabled ?? true,
  }
}
