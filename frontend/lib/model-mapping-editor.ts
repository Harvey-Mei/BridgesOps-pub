import type { GatewayModelMappingStrategy } from "@/lib/api-types"

export type ModelMappingDraft = {
  strategy: GatewayModelMappingStrategy
  model: string
  reasoningEffort: string
}

export type ModelMappingKeyFields = {
  model_mapping_strategy?: string | null
  model_mapping_model?: string | null
  model_mapping_reasoning_effort?: string | null
}

export type ModelMappingCandidateKeyFields = ModelMappingKeyFields & {
  app?: string | null
  model_family?: string | null
}

export type ModelMappingCandidateGroupFields = {
  id: number
  model_mapping_family?: string | null
  members?: Array<{
    channel_id: number
    group_name: string
  }>
}

export type ModelMappingCandidateFields = {
  channel_id: number
  group_name: string
  model_ids?: string[] | null
}

export type PersistedModelMappingState = {
  draft: ModelMappingDraft
  invalidStrategy: string | null
}

export type ModelMappingPatch = {
  model_mapping_strategy: GatewayModelMappingStrategy
  model_mapping_model: string
  model_mapping_reasoning_effort: string
}

const MODEL_MAPPING_STRATEGIES = new Set<GatewayModelMappingStrategy>([
  "off",
  "iq",
  "cost",
  "time",
  "balanced",
  "manual",
])

export function normalizeModelMappingStrategy(value: string | null | undefined): GatewayModelMappingStrategy {
  const normalized = String(value ?? "off").trim().toLowerCase() as GatewayModelMappingStrategy
  return MODEL_MAPPING_STRATEGIES.has(normalized) ? normalized : "off"
}

export function modelMappingDraftFromKey(key: ModelMappingKeyFields): ModelMappingDraft {
  return modelMappingStateFromKey(key).draft
}

export function modelMappingStateFromKey(key: ModelMappingKeyFields): PersistedModelMappingState {
  const rawStrategy = String(key.model_mapping_strategy ?? "").trim()
  const normalizedStrategy = rawStrategy.toLowerCase() as GatewayModelMappingStrategy
  const strategyValid = rawStrategy === "" || MODEL_MAPPING_STRATEGIES.has(normalizedStrategy)
  return {
    draft: {
      strategy: strategyValid ? normalizeModelMappingStrategy(rawStrategy) : "off",
      model: String(key.model_mapping_model ?? "").trim(),
      reasoningEffort: String(key.model_mapping_reasoning_effort ?? "").trim(),
    },
    invalidStrategy: strategyValid ? null : rawStrategy,
  }
}

export function modelMappingDraftIsValid(draft: ModelMappingDraft): boolean {
  if (draft.strategy === "off") return true
  if (draft.strategy === "manual") {
    return Boolean(draft.model.trim() && draft.reasoningEffort.trim())
  }
  return true
}

export function modelMappingDraftEqual(left: ModelMappingDraft, right: ModelMappingDraft): boolean {
  return left.strategy === right.strategy
    && left.model.trim() === right.model.trim()
    && left.reasoningEffort.trim() === right.reasoningEffort.trim()
}

export function modelMappingDraftIsDirty(draft: ModelMappingDraft, persisted: PersistedModelMappingState): boolean {
  return persisted.invalidStrategy !== null || !modelMappingDraftEqual(draft, persisted.draft)
}

export function modelMappingCandidateGroupCompatible(
  key: ModelMappingCandidateKeyFields,
  targetCandidateGroupID: string | number,
  groups: ModelMappingCandidateGroupFields[],
  candidates: ModelMappingCandidateFields[] = [],
): boolean {
  const persisted = modelMappingStateFromKey(key)
  if (persisted.invalidStrategy !== null) return false
  return modelMappingDraftCandidateGroupCompatible(key, persisted.draft, targetCandidateGroupID, groups, candidates)
}

export function modelMappingDraftCandidateGroupCompatible(
  key: ModelMappingCandidateKeyFields,
  draft: ModelMappingDraft,
  targetCandidateGroupID: string | number,
  groups: ModelMappingCandidateGroupFields[],
  candidates: ModelMappingCandidateFields[] = [],
): boolean {
  if (draft.strategy === "off") return true

  const groupID = normalizeCandidateGroupID(targetCandidateGroupID)
  if (!Number.isFinite(groupID) || groupID <= 0) return false

  const group = groups.find((item) => item.id === groupID)
  if (!group) return false
  const groupFamily = String(group.model_mapping_family ?? "").trim().toLowerCase()
  if (groupFamily !== "codex" && groupFamily !== "claude") return false

  const configuredFamily = String(key.model_family ?? "").trim().toLowerCase()
  const app = String(key.app ?? "").trim().toLowerCase()
  const keyFamily = configuredFamily || (app === "claude" || app === "gemini" ? app : "codex")
  if (groupFamily !== keyFamily) return false
  if (draft.strategy !== "manual") return true

  const fixedModel = draft.model.trim()
  if (!fixedModel) return false
  const normalizedFixedModel = fixedModel.toLowerCase()
  const memberKeys = new Set((group.members ?? []).map((member) => (
    `${member.channel_id}::${String(member.group_name ?? "").trim()}`
  )))
  return candidates.some((candidate) => (
    memberKeys.has(`${candidate.channel_id}::${String(candidate.group_name ?? "").trim()}`)
    && (candidate.model_ids ?? []).some((model) => String(model ?? "").trim().toLowerCase() === normalizedFixedModel)
  ))
}

export function candidateGroupSelectionChanged(
  currentCandidateGroupID: number | null | undefined,
  targetCandidateGroupID: string | number,
): boolean {
  return Number(currentCandidateGroupID ?? 0) !== normalizeCandidateGroupID(targetCandidateGroupID)
}

function normalizeCandidateGroupID(value: string | number): number {
  return typeof value === "string" && value === "__all__" ? 0 : Number(value)
}

export function buildModelMappingPatch(draft: ModelMappingDraft): ModelMappingPatch {
  return {
    model_mapping_strategy: draft.strategy,
    model_mapping_model: draft.strategy === "manual" ? draft.model.trim() : "",
    model_mapping_reasoning_effort: draft.strategy === "off" ? "" : draft.reasoningEffort.trim(),
  }
}

export function setModelMappingEnabled(
  current: ModelMappingDraft,
  enabled: boolean,
  app: string,
): ModelMappingDraft {
  if (!enabled) {
    return { strategy: "off", model: "", reasoningEffort: "" }
  }
  if (current.strategy !== "off") return current
  return {
    strategy: app.trim().toLowerCase() === "claude" ? "manual" : "balanced",
    model: "",
    reasoningEffort: "",
  }
}
