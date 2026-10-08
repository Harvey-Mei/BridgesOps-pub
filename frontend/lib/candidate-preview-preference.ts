const candidatePreviewModelStoragePrefix = "bridgeops.gateway.candidate-preview-model.v1."

export type CandidatePreviewPreferenceStorage = Pick<Storage, "getItem" | "setItem" | "removeItem">
export type CandidatePreviewPreferenceContext = "requested" | "mapped"
export type CandidatePreviewModelPreferenceSource = "saved" | "mapping" | "recent_request" | "available_default" | "none"

export type CandidatePreviewModelPreferenceResolution = {
  model: string
  context: CandidatePreviewPreferenceContext
  source: CandidatePreviewModelPreferenceSource
}

function candidatePreviewPreferenceStorage(
  storage?: CandidatePreviewPreferenceStorage | null,
): CandidatePreviewPreferenceStorage | null {
  if (storage !== undefined) return storage
  if (typeof window === "undefined") return null
  try {
    return window.localStorage
  } catch {
    return null
  }
}

export function candidatePreviewModelStorageKey(
  downstreamKeyID: number,
  context: CandidatePreviewPreferenceContext,
) {
  if (!Number.isSafeInteger(downstreamKeyID) || downstreamKeyID <= 0) return ""
  return `${candidatePreviewModelStoragePrefix}${downstreamKeyID}.${context}`
}

export function readCandidatePreviewModelPreference(
  downstreamKeyID: number,
  context: CandidatePreviewPreferenceContext,
  storage?: CandidatePreviewPreferenceStorage | null,
) {
  const key = candidatePreviewModelStorageKey(downstreamKeyID, context)
  const target = candidatePreviewPreferenceStorage(storage)
  if (!key || !target) return ""
  try {
    return String(target.getItem(key) ?? "").trim()
  } catch {
    return ""
  }
}

export function writeCandidatePreviewModelPreference(
  downstreamKeyID: number,
  context: CandidatePreviewPreferenceContext,
  model: string,
  storage?: CandidatePreviewPreferenceStorage | null,
) {
  const key = candidatePreviewModelStorageKey(downstreamKeyID, context)
  const value = model.trim()
  const target = candidatePreviewPreferenceStorage(storage)
  if (!key || !value || !target) return false
  try {
    target.setItem(key, value)
    return true
  } catch {
    return false
  }
}

function currentCandidateModel(models: readonly string[], requested: string) {
  const normalized = requested.trim().toLowerCase()
  if (!normalized) return ""
  return models.find((model) => model.trim().toLowerCase() === normalized)?.trim() ?? ""
}

export function resolveCandidatePreviewModelPreference({
  downstreamKeyID,
  validModels,
  preferenceContext,
  mappedModel,
  recentRequestModel,
  defaultContext = preferenceContext,
  storage,
}: {
  downstreamKeyID: number
  validModels: readonly string[]
  preferenceContext: CandidatePreviewPreferenceContext
  mappedModel?: string | null
  recentRequestModel?: string | null
  defaultContext?: CandidatePreviewPreferenceContext
  storage?: CandidatePreviewPreferenceStorage | null
}): CandidatePreviewModelPreferenceResolution {
  const models = Array.from(new Set(validModels.map((model) => model.trim()).filter(Boolean)))
  if (models.length === 0) {
    return { model: "", context: defaultContext, source: "none" }
  }
  const savedModel = readCandidatePreviewModelPreference(downstreamKeyID, preferenceContext, storage)
  const validSavedModel = currentCandidateModel(models, savedModel)
  if (validSavedModel) {
    return { model: validSavedModel, context: preferenceContext, source: "saved" }
  }
  // Candidate inventories refresh independently. A missing value is not
  // sufficient evidence to erase the user's saved preference.

  const validMappedModel = currentCandidateModel(models, String(mappedModel ?? ""))
  if (validMappedModel) {
    return { model: validMappedModel, context: "mapped", source: "mapping" }
  }
  const validRecentModel = currentCandidateModel(models, String(recentRequestModel ?? ""))
  if (validRecentModel) {
    return { model: validRecentModel, context: defaultContext, source: "recent_request" }
  }
  return { model: models[0], context: defaultContext, source: "available_default" }
}
