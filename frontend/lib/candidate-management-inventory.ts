import type {
  GatewayApp,
  GatewayCandidateGroup,
  GatewayDownstreamKey,
  GatewayRouteCandidate,
} from "./api-types"

/**
 * Candidate management is an inventory surface, not merely a routing preview.
 * It must keep every saved member visible while model-qualified preview data is
 * loading or unavailable, including members whose current rate snapshot has
 * disappeared. Missing snapshots use a separate identity-only variant: they
 * must never impersonate a live route with a guessed channel type or zero
 * price. The model preview, when available, overlays only the matching
 * protocol projection instead of replacing the static inventory.
 */
export type CandidateManagementCurrentCandidate = GatewayRouteCandidate & {
  kind: "current"
  membership_snapshot_missing?: false
}

export type CandidateManagementMissingSnapshotCandidate = {
  kind: "missing_snapshot"
  membership_snapshot_missing: true
  upstream_group_id?: number
  presence_generation?: number
  channel_id: number
  channel_name: string
  channel_type: "unknown"
  gateway_app: GatewayApp
  group_name: string
  disabled_for_key: boolean
  cooling_down: false
  routing_available: false
  availability_reasons: ["no_rate_snapshot"]
}

export type CandidateManagementInventoryCandidate =
  | CandidateManagementCurrentCandidate
  | CandidateManagementMissingSnapshotCandidate

export function candidateManagementInventoryCandidateIsCurrent(
  candidate: CandidateManagementInventoryCandidate,
): candidate is CandidateManagementCurrentCandidate {
  return candidate.kind === "current"
}

export function candidateManagementMemberKey(channelID: number, groupName: string | null | undefined) {
  return `${channelID}::${normalizeGroupName(groupName)}`
}

export function candidateManagementRankedMemberKeys(
  memberKeys: ReadonlySet<string>,
  previewCandidates: readonly GatewayRouteCandidate[],
  inventoryCandidates: readonly GatewayRouteCandidate[],
) {
  const rankedKeys: string[] = []
  const seen = new Set<string>()
  for (const candidates of [previewCandidates, inventoryCandidates]) {
    for (const candidate of candidates) {
      const key = candidateManagementMemberKey(candidate.channel_id, candidate.group_name)
      if (!memberKeys.has(key) || seen.has(key)) continue
      seen.add(key)
      rankedKeys.push(key)
    }
  }
  return rankedKeys
}

export function candidateManagementInventoryCandidates(
  key: GatewayDownstreamKey,
  candidateGroups: readonly GatewayCandidateGroup[],
  inventoryCandidates: readonly GatewayRouteCandidate[],
  previewCandidates: readonly GatewayRouteCandidate[],
  disabledOverrides: ReadonlyMap<string, boolean> = new Map(),
): CandidateManagementInventoryCandidate[] {
  const group = candidateGroups.find((candidateGroup) => candidateGroup.id === key.candidate_group_id)
  if (!group) return []

  const members = uniqueMembers(group)
  const memberKeys = new Set(members.map((member) => candidateManagementMemberKey(member.channel_id, member.group_name)))
  const exclusionKeys = new Set((key.route_exclusions ?? []).map((exclusion) => (
    candidateManagementMemberKey(exclusion.channel_id, exclusion.group_name)
  )))
  const inventoryByMember = candidatesByMember(inventoryCandidates, memberKeys)
  const previewByMember = candidatesByMember(previewCandidates, memberKeys)
  const result: CandidateManagementInventoryCandidate[] = []

  for (const member of members) {
    const memberKey = candidateManagementMemberKey(member.channel_id, member.group_name)
    const inventory = inventoryByMember.get(memberKey) ?? []
    const preview = previewByMember.get(memberKey) ?? []
    const previewByProjection = new Map(preview.map((candidate) => [
      candidateProjectionKey(candidate),
      candidate,
    ]))
    const candidates: CandidateManagementInventoryCandidate[] = inventory.length > 0
      ? inventory.map((candidate) => {
        const previewCandidate = previewByProjection.get(candidateProjectionKey(candidate))
        return {
          ...(previewCandidate ? mergePreviewCandidate(candidate, previewCandidate) : candidate),
          kind: "current",
        }
      })
      : preview.length > 0
        ? preview.map((candidate) => ({ ...candidate, kind: "current" }))
        : [missingSnapshotCandidate(key, member.channel_id, member.group_name, member.upstream_group_id, member.presence_generation)]

    const disabledOverride = disabledOverrides.get(memberKey)
    const excluded = exclusionKeys.has(memberKey)
    for (const candidate of candidates) {
      result.push({
        ...candidate,
        disabled_for_key: disabledOverride ?? (candidate.disabled_for_key || excluded),
      })
    }
  }

  return result
}

function uniqueMembers(group: GatewayCandidateGroup) {
  const members = new Map<string, GatewayCandidateGroup["members"][number]>()
  for (const member of group.members ?? []) {
    const key = candidateManagementMemberKey(member.channel_id, member.group_name)
    if (!members.has(key)) members.set(key, member)
  }
  return Array.from(members.values())
}

function candidatesByMember(
  candidates: readonly GatewayRouteCandidate[],
  memberKeys: ReadonlySet<string>,
) {
  const byMember = new Map<string, GatewayRouteCandidate[]>()
  for (const candidate of candidates) {
    const memberKey = candidateManagementMemberKey(candidate.channel_id, candidate.group_name)
    if (!memberKeys.has(memberKey)) continue
    const existing = byMember.get(memberKey)
    if (existing) {
      if (!existing.some((item) => candidateProjectionKey(item) === candidateProjectionKey(candidate))) existing.push(candidate)
    } else {
      byMember.set(memberKey, [candidate])
    }
  }
  return byMember
}

function candidateProjectionKey(candidate: GatewayRouteCandidate) {
  return `${candidateManagementMemberKey(candidate.channel_id, candidate.group_name)}::${candidate.gateway_app}`
}

function mergePreviewCandidate(inventory: GatewayRouteCandidate, preview: GatewayRouteCandidate): GatewayRouteCandidate {
  // The preview is evaluated for the active downstream key and selected model,
  // while the inventory retains every synchronized protocol projection so one
  // channel/group remains a single aggregated management row.
  return {
    ...inventory,
    ...preview,
    gateway_app: inventory.gateway_app,
  }
}

function missingSnapshotCandidate(
  key: GatewayDownstreamKey,
  channelID: number,
  groupName: string,
  upstreamGroupID: number,
  presenceGeneration: number | undefined,
): CandidateManagementInventoryCandidate {
  return {
    kind: "missing_snapshot",
    upstream_group_id: upstreamGroupID || undefined,
    presence_generation: presenceGeneration,
    channel_id: channelID,
    channel_name: `渠道 ${channelID}`,
    channel_type: "unknown",
    gateway_app: key.app,
    group_name: groupName,
    disabled_for_key: false,
    cooling_down: false,
    routing_available: false,
    availability_reasons: ["no_rate_snapshot"],
    membership_snapshot_missing: true,
  }
}

function normalizeGroupName(value: string | null | undefined) {
  return String(value ?? "").trim().toLowerCase()
}
