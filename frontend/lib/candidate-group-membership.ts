export const candidateGroupMembershipFilterAll = "__all__"

const candidateGroupMembershipCollator = new Intl.Collator("zh-CN", { numeric: true, sensitivity: "base" })

export type CandidateGroupMembershipStatusFilter =
  | typeof candidateGroupMembershipFilterAll
  | "routing_available"
  | "cooling_down"
  | "routing_unavailable"

export type CandidateGroupMembershipBulkOperation = "add" | "remove"

export type CandidateGroupMembershipIdentity = {
  channel_id: number
  group_name: string
  upstream_group_id?: number
  presence_generation?: number
}

export type CandidateGroupMembershipCandidate = {
  channel_id: number
  channel_name?: string | null
  channel_type?: string | null
  gateway_app?: string | null
  group_name?: string | null
  disabled_for_key?: boolean
  cooling_down?: boolean
  upstream_group_id?: number
  presence_generation?: number
  upstream_key_configured?: boolean
  upstream_key_enabled?: boolean
  upstream_key_provisioning?: boolean
  upstream_key_available?: boolean
  routing_available?: boolean
  availability_reasons?: string[]
  membership_snapshot_missing?: boolean
}

export type CandidateGroupMembershipRow<T extends CandidateGroupMembershipCandidate = CandidateGroupMembershipCandidate> = {
  key: string
  channelID: number
  channelName: string
  channelType: string
  groupName: string
  gatewayApps: string[]
  coolingDown: boolean
  routingAvailable: boolean
  snapshotMissing: boolean
  availabilityReasons: string[]
  candidates: T[]
}

export type CandidateGroupMembershipFilters = {
  search: string
  status: CandidateGroupMembershipStatusFilter
  protocol: string
  channelID: string
}

export type CandidateGroupMembershipChannel<T extends CandidateGroupMembershipCandidate = CandidateGroupMembershipCandidate> = {
  channelID: number
  channelName: string
  channelType: string
  rows: CandidateGroupMembershipRow<T>[]
}

export type CandidateGroupMembershipChannelOption = {
  value: string
  label: string
  channelType: string
}

export type CandidateGroupMembershipRowState = {
  coolingDown: boolean
  routingAvailable: boolean
  routingUnavailable: boolean
  snapshotMissing: boolean
  availabilityReasons: string[]
}

export function candidateGroupMemberKey(channelID: number, groupName: string | null | undefined) {
  return `${channelID}::${normalize(groupName)}`
}

export function buildCandidateGroupMembershipRows<T extends CandidateGroupMembershipCandidate>(
  candidates: readonly T[],
): CandidateGroupMembershipRow<T>[] {
  const grouped = new Map<string, T[]>()
  for (const candidate of candidates) {
    const key = candidateGroupMemberKey(candidate.channel_id, candidate.group_name)
    const current = grouped.get(key)
    if (current) current.push(candidate)
    else grouped.set(key, [candidate])
  }

  return Array.from(grouped, ([key, sources]) => {
    const first = sources[0]
    const gatewayApps = uniqueSorted(sources.map((candidate) => candidate.gateway_app))
    const availabilityReasons = candidateMembershipAvailabilityReasons(sources)
    return {
      key,
      channelID: first.channel_id,
      channelName: displayChannelName(first),
      channelType: firstNonEmpty(sources.map((candidate) => candidate.channel_type)),
      groupName: displayGroupName(first),
      gatewayApps,
      coolingDown: sources.some((candidate) => candidate.cooling_down === true),
      routingAvailable: sources.some(candidateRoutingAvailable),
      snapshotMissing: sources.some((candidate) => candidate.membership_snapshot_missing === true),
      availabilityReasons,
      candidates: sources,
    }
  }).sort(compareMembershipRows)
}

export function filterCandidateGroupMembershipRows<T extends CandidateGroupMembershipCandidate>(
  rows: readonly CandidateGroupMembershipRow<T>[],
  filters: CandidateGroupMembershipFilters,
) {
  const channelID = filters.channelID === candidateGroupMembershipFilterAll ? "" : filters.channelID.trim()
  const protocol = filters.protocol === candidateGroupMembershipFilterAll ? "" : normalize(filters.protocol)
  const searchTokens = normalize(filters.search).split(/\s+/).filter(Boolean)

  return rows.filter((row) => {
    if (channelID && String(row.channelID) !== channelID) return false
    const protocolCandidates = protocol
      ? row.candidates.filter((candidate) => normalize(candidate.gateway_app) === protocol)
      : row.candidates
    if (protocolCandidates.length === 0) return false
    if (!candidateGroupMembershipStatusMatches(protocolCandidates, filters.status)) return false
    if (searchTokens.length === 0) return true
    const searchable = normalize([
      row.channelName,
      row.channelType,
      row.groupName,
      ...row.gatewayApps,
    ].join(" "))
    return searchTokens.every((token) => searchable.includes(token))
  })
}

export function groupCandidateGroupMembershipRows<T extends CandidateGroupMembershipCandidate>(
  rows: readonly CandidateGroupMembershipRow<T>[],
): CandidateGroupMembershipChannel<T>[] {
  const channels = new Map<number, CandidateGroupMembershipChannel<T>>()
  for (const row of rows) {
    const existing = channels.get(row.channelID)
    if (existing) existing.rows.push(row)
    else channels.set(row.channelID, {
      channelID: row.channelID,
      channelName: row.channelName,
      channelType: row.channelType,
      rows: [row],
    })
  }
  return Array.from(channels.values()).sort(compareMembershipChannels)
}

export function candidateGroupMembershipProtocolOptions<T extends CandidateGroupMembershipCandidate>(rows: readonly CandidateGroupMembershipRow<T>[]) {
  return uniqueSorted(rows.flatMap((row) => row.gatewayApps))
}

export function candidateGroupMembershipChannelOptions<T extends CandidateGroupMembershipCandidate>(
  rows: readonly CandidateGroupMembershipRow<T>[],
): CandidateGroupMembershipChannelOption[] {
  return groupCandidateGroupMembershipRows(rows).map((channel) => ({
    value: String(channel.channelID),
    label: channel.channelName,
    channelType: channel.channelType,
  }))
}

export function candidateGroupMembershipBulkTargets<T extends CandidateGroupMembershipCandidate>(
  rows: readonly CandidateGroupMembershipRow<T>[],
  selectedKeys: ReadonlySet<string>,
  operation: CandidateGroupMembershipBulkOperation,
) {
  return rows.filter((row) => operation === "add" ? !selectedKeys.has(row.key) : selectedKeys.has(row.key))
}

export function resolveCandidateGroupMembershipSelection(
  existingMembers: readonly CandidateGroupMembershipIdentity[],
  candidates: readonly CandidateGroupMembershipCandidate[],
  selectedKeys: ReadonlySet<string>,
) {
  const memberByKey = new Map(existingMembers.map((member) => [
    candidateGroupMemberKey(member.channel_id, member.group_name),
    member,
  ]))
  for (const candidate of candidates) {
    const groupName = String(candidate.group_name ?? "")
    const member: CandidateGroupMembershipIdentity = {
      channel_id: candidate.channel_id,
      group_name: groupName,
    }
    if (candidate.upstream_group_id && candidate.presence_generation) {
      member.upstream_group_id = candidate.upstream_group_id
      member.presence_generation = candidate.presence_generation
    }
    memberByKey.set(candidateGroupMemberKey(candidate.channel_id, groupName), member)
  }
  const members: CandidateGroupMembershipIdentity[] = []
  const unresolvedKeys: string[] = []
  for (const key of selectedKeys) {
    const member = memberByKey.get(key)
    if (member) members.push(member)
    else unresolvedKeys.push(key)
  }
  return { members, unresolvedKeys }
}

export function candidateGroupMembershipRowState<T extends CandidateGroupMembershipCandidate>(
  row: CandidateGroupMembershipRow<T>,
  protocol = "",
): CandidateGroupMembershipRowState {
  const normalizedProtocol = protocol === candidateGroupMembershipFilterAll ? "" : normalize(protocol)
  const candidates = normalizedProtocol
    ? row.candidates.filter((candidate) => normalize(candidate.gateway_app) === normalizedProtocol)
    : row.candidates
  return {
    coolingDown: candidates.some((candidate) => candidate.cooling_down === true),
    routingAvailable: candidates.some(candidateRoutingAvailable),
    routingUnavailable: candidates.some((candidate) => !candidateRoutingAvailable(candidate)),
    snapshotMissing: candidates.some((candidate) => candidate.membership_snapshot_missing === true),
    availabilityReasons: candidateMembershipAvailabilityReasons(candidates),
  }
}

function candidateGroupMembershipStatusMatches(
  candidates: readonly CandidateGroupMembershipCandidate[],
  status: CandidateGroupMembershipStatusFilter,
) {
  switch (status) {
    case "routing_available":
      return candidates.some(candidateRoutingAvailable)
    case "cooling_down":
      return candidates.some((candidate) => candidate.cooling_down === true)
    case "routing_unavailable":
      return candidates.some((candidate) => !candidateRoutingAvailable(candidate))
    default:
      return true
  }
}

function candidateRoutingAvailable(candidate: CandidateGroupMembershipCandidate) {
  if (candidate.membership_snapshot_missing === true) return false
  if (candidate.cooling_down === true) return false
  if (typeof candidate.routing_available === "boolean") return candidate.routing_available
  return candidateMembershipAvailabilityReasons([candidate]).length === 0
}

function candidateMembershipAvailabilityReasons(candidates: readonly CandidateGroupMembershipCandidate[]) {
  return uniqueSorted(candidates.flatMap((candidate) => candidate.availability_reasons ?? []))
    .filter((reason) => reason !== "probe_disabled")
}

function displayChannelName(candidate: CandidateGroupMembershipCandidate) {
  return String(candidate.channel_name ?? "").trim() || `渠道 ${candidate.channel_id}`
}

function displayGroupName(candidate: CandidateGroupMembershipCandidate) {
  return String(candidate.group_name ?? "").trim() || "默认分组"
}

function firstNonEmpty(values: readonly (string | null | undefined)[]) {
  return values.map((value) => String(value ?? "").trim()).find(Boolean) ?? ""
}

function uniqueSorted(values: readonly (string | null | undefined)[]) {
  return Array.from(new Set(values.map(normalize).filter(Boolean))).sort(compareText)
}

function normalize(value: string | null | undefined) {
  return String(value ?? "").trim().toLowerCase()
}

function compareMembershipRows<T extends CandidateGroupMembershipCandidate>(left: CandidateGroupMembershipRow<T>, right: CandidateGroupMembershipRow<T>) {
  return compareText(left.channelName, right.channelName)
    || left.channelID - right.channelID
    || compareText(left.groupName, right.groupName)
    || compareText(left.key, right.key)
}

function compareMembershipChannels<T extends CandidateGroupMembershipCandidate>(left: CandidateGroupMembershipChannel<T>, right: CandidateGroupMembershipChannel<T>) {
  return compareText(left.channelName, right.channelName) || left.channelID - right.channelID
}

function compareText(left: string, right: string) {
  return candidateGroupMembershipCollator.compare(left, right) || (left < right ? -1 : left > right ? 1 : 0)
}
